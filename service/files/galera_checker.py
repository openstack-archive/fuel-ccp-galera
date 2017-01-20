#!/usr/bin/env python

import argparse
import BaseHTTPServer
import functools
import json
import logging
import os
import os.path
import socket
import sys
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
DATADIR = "/var/lib/mysql"
SST_FLAG = os.path.join(DATADIR, "sst_in_progress")
PID_FILE = os.path.join(DATADIR, "mysqld.pid")
HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)

MONITOR_PASSWORD = None
CLUSTER_NAME = None
ETCD_PATH = None
ETCD_HOST = None
ETCD_PORT = None


def retry(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = 3
        delay = 1
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

        etcd_client = etcd.Client(host=ETCD_HOST,
                                  port=ETCD_PORT,
                                  allow_reconnect=True,
                                  read_timeout=2)
        return etcd_client


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

        return True if os.path.isfile(SST_FLAG) else False

    def check_if_pidfile_created(self):

        return True if os.path.isfile(PID_FILE) else False

    def check_if_galera_ready(self):

        self.check_cluster_state()
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

    def check_cluster_state(self):

        state = self._etcd_read('state')
        if state != 'STEADY':
            LOG.error("Cluster state is not STEADY")
            sys.exit(1)


class GaleraHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):

        LOG.debug("Started processing GET request")
        try:
            checker = GaleraChecker()
            alive = checker.check_if_galera_alive()
            state = 200 if alive else 503
            self.send_response(state)
            self.end_headers()
        except Exception as err:
            LOG.exception(err)
            self.send_response(503)
            self.end_headers()
        finally:
            LOG.debug("Finished processing GET request")


def run_liveness(port=8080):
    server_class = BaseHTTPServer.HTTPServer
    handler_class = GaleraHttpHandler
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    LOG.info('Starting http server...')
    httpd.serve_forever()


def run_readiness():
    checker = GaleraChecker()
    ready = checker.check_if_galera_ready()
    sys.exit(0) if ready else sys.exit(1)


def get_config():

    LOG.info("Getting global variables from %s", GLOBALS_PATH)
    variables = {}
    with open(GLOBALS_PATH) as f:
        global_conf = json.load(f)
    for key in ['percona', 'etcd', 'namespace']:
        variables[key] = global_conf[key]
    LOG.debug(variables)
    return variables


def set_globals():

    config = get_config()
    global MONITOR_PASSWORD, CLUSTER_NAME
    global ETCD_PATH, ETCD_HOST, ETCD_PORT

    CLUSTER_NAME = config['percona']['cluster_name']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s.svc.cluster.local" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('type', choices=['liveness', 'readiness'])
    args = parser.parse_args()

    get_config()
    set_globals()
    if args.type == 'liveness':
        run_liveness()
    elif args.type == 'readiness':
        run_readiness()
