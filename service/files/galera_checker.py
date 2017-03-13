#!/usr/bin/env python

import BaseHTTPServer
import functools
import json
import logging
import os
import os.path
import socket
import time

import etcd
import pymysql.cursors

# Galera states
JOINING_STATE = 1
DONOR_DESYNCED_STATE = 2
JOINED_STATE = 3
SYNCED_STATE = 4
WAS_JOINED = False
OLD_STATE = 0

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

GLOBALS_PATH = "/etc/ccp/globals/globals.json"
GLOBALS_SECRETS_PATH = '/etc/ccp/global-secrets/global-secrets.json'
DATADIR = "/var/lib/mysql"
SST_FLAG = os.path.join(DATADIR, "sst_in_progress")
PID_FILE = os.path.join(DATADIR, "mysqld.pid")
HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)
CA_CERT = '/opt/ccp/etc/tls/ca.pem'

MONITOR_PASSWORD = None
CLUSTER_NAME = None
ETCD_PATH = None
ETCD_HOST = None
ETCD_PORT = None
ETCD_TLS = None


def retry(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = 3
        delay = 5
        while attempts > 1:
            try:
                return f(*args, **kwargs)
            except etcd.EtcdException as e:
                LOG.warning('Etcd is not ready: %s', str(e))
                LOG.warning('Retrying in %d seconds...', delay)
                time.sleep(delay)
                attempts -= 1
            except pymysql.OperationalError as e:
                LOG.warning('Mysql is not ready: %s', str(e))
                LOG.warning('Retrying in %d seconds...', delay)
                time.sleep(delay)
                attempts -= 1
        return f(*args, **kwargs)
    return wrap


def get_etcd_client():

    if ETCD_TLS:
        protocol = 'https'
        ca_cert = CA_CERT
    else:
        protocol = 'http'
        ca_cert = None

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       protocol=protocol,
                       ca_cert=ca_cert,
                       read_timeout=2)


@retry
def get_mysql_client():
        mysql_client = pymysql.connect(host='127.0.0.1',
                                       port=33306,
                                       user='monitor',
                                       password=MONITOR_PASSWORD,
                                       connect_timeout=1,
                                       read_timeout=1,
                                       cursorclass=pymysql.cursors.DictCursor)
        return mysql_client


class GaleraChecker(object):
    def __init__(self):
        self.etcd_client = get_etcd_client()
        # Liveness check runs every 10 seconds with 5 seconds timeout (default)
        self.ttl = 20

    @retry
    def fetch_wsrep_data(self):
        data = {}
        mysql_client = get_mysql_client()
        with mysql_client.cursor() as cursor:
            sql = "SHOW STATUS LIKE 'wsrep%'"
            cursor.execute(sql)
            for i in cursor.fetchall():
                data[i['Variable_name']] = i['Value']
            return data

    def check_if_sst_running(self):

        return os.path.isfile(SST_FLAG)

    def check_if_pidfile_created(self):

        return True if os.path.isfile(PID_FILE) else False

    def check_if_galera_ready(self):

        state = self.fetch_cluster_state()
        if state != 'STEADY':
            LOG.error("Cluster state is not STEADY")
            return False
        wsrep_data = self.fetch_wsrep_data()
        uuid = self.etcd_get_cluster_uuid()

        if wsrep_data["wsrep_local_state_comment"] != "Synced":
            LOG.error("wsrep_local_state_comment != 'Synced' - '%s'",
                      wsrep_data["wsrep_local_state_comment"])
            return False
        elif wsrep_data["wsrep_evs_state"] != "OPERATIONAL":
            LOG.error("wsrep_evs_state != 'OPERATIONAL' - '%s'",
                      wsrep_data["wsrep_evs_state"])
            return False
        elif wsrep_data["wsrep_connected"] != "ON":
            LOG.error("wsrep_connected != 'ON' - '%s'",
                      wsrep_data["wsrep_connected"])
            return False
        elif wsrep_data["wsrep_ready"] != "ON":
            LOG.error("wsrep_ready != 'ON' - '%s'",
                      wsrep_data["wsrep_ready"])
            return False
        elif wsrep_data["wsrep_cluster_state_uuid"] != uuid:
            LOG.error("wsrep_cluster_state_uuid != '%s' - '%s'",
                      uuid, wsrep_data["wsrep_cluster_state_uuid"])
            return False
        else:
            LOG.info("Galera node is ready")
            return True

    def check_if_galera_alive(self):

        # If cluster is not STEADY, nodes could be in strange positions,
        # like SST sync. We should postpone liveness checks 'till bootstrap is
        # done
        if not self.etcd_check_if_cluster_ready():
            LOG.info("Galera cluster status is not 'STEADY', skiping check")
            return True

        # During SST sync mysql can't accept any requests
        if self.check_if_sst_running():
            LOG.info("SST sync in progress, skiping check")
            return True

        if not self.check_if_pidfile_created():
            LOG.info("Mysql pid file is not yet created, skiping check")
            return True

        global WAS_JOINED
        global OLD_STATE
        wsrep_data = self.fetch_wsrep_data()

        # If local uuid is different - we have a split brain.
        cluster_uuid = self.etcd_get_cluster_uuid()
        mysql_uuid = wsrep_data['wsrep_cluster_state_uuid']
        if cluster_uuid != mysql_uuid:
            LOG.error("Cluster uuid is differs from local one.")
            LOG.debug("Cluster uuid: %s Local uuid: %s",
                      cluster_uuid, mysql_uuid)
            return False

        # Node states check.
        state = int(wsrep_data['wsrep_local_state'])
        state_comment = wsrep_data['wsrep_local_state_comment']

        if state == SYNCED_STATE or state == DONOR_DESYNCED_STATE:
            WAS_JOINED = True
            LOG.info("State OK: %s", state_comment)
            self.etcd_register_in_path('nodes')
            return True
        elif state == JOINED_STATE and WAS_JOINED:
            # Node was in the JOINED_STATE in prev check too. Seems to it can't
            # start syncing.
            if OLD_STATE == JOINED_STATE:
                LOG.error("State BAD: %s", state_comment)
                LOG.error("Joined, but not syncing")
                self._etcd_delete()
                return False
            else:
                LOG.info("State OK: %s", state_comment)
                LOG.info("Probably will sync soon")
                self.etcd_register_in_path('nodes')
                return False
        else:
            LOG.info("State OK: %s", state_comment)
            LOG.info("Just joined")
            WAS_JOINED = True
            self.etcd_register_in_path('nodes')
            return True
        OLD_STATE = state
        LOG.warning("Unknown state: %s", state_comment)
        return True

    @retry
    def _etcd_delete(self):

        key = os.path.join(ETCD_PATH, 'nodes', IPADDR)
        self.etcd_client.delete(key, recursive=True, dir=True)
        LOG.warning("Deleted node's key '%s'", key)

    @retry
    def _etcd_set(self, data):

        self.etcd_client.set(data[0], data[1], self.ttl)
        LOG.info("Set %s with value '%s'", data[0], data[1])

    @retry
    def _etcd_read(self, path):

        key = os.path.join(ETCD_PATH, path)
        return self.etcd_client.read(key).value

    def etcd_register_in_path(self, path):

        key = os.path.join(ETCD_PATH, path, IPADDR)
        self._etcd_set((key, time.time()))

    def etcd_check_if_cluster_ready(self):

        try:
            state = self._etcd_read('state')
            return True if state == 'STEADY' else False
        except etcd.EtcdKeyNotFound:
            return False

    def etcd_get_cluster_uuid(self):

        return self._etcd_read('uuid')

    def fetch_cluster_state(self):

        return self._etcd_read('state')


class GaleraHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):

        uri = self.path
        LOG.debug("Started processing GET '%s' request", uri)
        checker = GaleraChecker()
        try:
            if uri == "/liveness":
                success = checker.check_if_galera_alive()
            elif uri == "/readiness":
                success = checker.check_if_galera_ready()
            else:
                LOG.error("Only '/liveness' and '/readiness' uri are"
                          " supported")
                success = False
            response = 200 if success else 503
            self.send_response(response)
            self.end_headers()
        except Exception as err:
            LOG.exception(err)
            self.send_response(503)
            self.end_headers()
        finally:
            LOG.debug("Finished processing GET request")


def run_server(port=8080):
    server_class = BaseHTTPServer.HTTPServer
    handler_class = GaleraHttpHandler
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    LOG.info('Starting http server...')
    httpd.serve_forever()


def merge_configs(variables, new_config):
    for k, v in new_config.items():
        if k not in variables:
            variables[k] = v
            continue
        if isinstance(v, dict) and isinstance(variables[k], dict):
            merge_configs(variables[k], v)
        else:
            variables[k] = v


def get_config():

    LOG.info("Getting global variables from %s", GLOBALS_PATH)
    variables = {}
    with open(GLOBALS_PATH) as f:
        global_conf = json.load(f)
    with open(GLOBALS_SECRETS_PATH) as f:
        secrets = json.load(f)
    merge_configs(global_conf, secrets)
    for key in ['percona', 'etcd', 'namespace', 'cluster_domain']:
        variables[key] = global_conf[key]
    LOG.debug(variables)
    return variables


def set_globals():

    config = get_config()
    global MONITOR_PASSWORD, CLUSTER_NAME
    global ETCD_PATH, ETCD_HOST, ETCD_PORT, ETCD_TLS

    CLUSTER_NAME = config['percona']['cluster_name']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s.svc.%s" % (config['namespace'],
                                    config['cluster_domain'])
    ETCD_PORT = int(config['etcd']['client_port']['cont'])
    ETCD_TLS = config['etcd']['tls']['enabled']


if __name__ == "__main__":

    get_config()
    set_globals()
    run_server()
