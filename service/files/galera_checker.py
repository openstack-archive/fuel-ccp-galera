#!/usr/bin/env python

import argparse
import BaseHTTPServer
import functools
import json
import logging
import os
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

GLOBALS_PATH = '/etc/ccp/globals/globals.json'
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
                                       port=3306,
                                       user='monitor',
                                       password=MONITOR_PASSWORD,
                                       connect_timeout=1,
                                       read_timeout=1,
                                       cursorclass=pymysql.cursors.DictCursor)
        return mysql_client


class GaleraChecker(object):
    def __init__(self):
        self.etcd_client = get_etcd_client()
        self.ttl = 30

    @retry
    def fetch_mysql_data(self):

        with self.mysql_client.cursor() as cursor:
            sql = "SHOW STATUS LIKE 'wsrep%'"
            cursor.execute(sql)
            data = cursor.fetchall()
            return data

    def check_galera_state(self):

        # If cluster is not STEADY, nodes could be in strange positions,
        # like SST sync. We should postpone liveness checks 'till bootstrap is
        # done
        if not self.etcd_check_if_cluster_ready():
            LOG.info("Galera cluster status is not 'STEADY', skiping check")
            self.etcd_register_in_path('nodes')
            return 200

        global WAS_JOINED
        global OLD_STATE
        wsrep_data = {}
        self.mysql_client = get_mysql_client()

        data = self.fetch_mysql_data()
        for i in data:
            wsrep_data[i['Variable_name']] = i['Value']

        # If local uuid is different - we have a split brain.
        cluster_uuid = self.etcd_get_cluster_uuid()
        mysql_uuid = wsrep_data['wsrep_cluster_state_uuid']
        if cluster_uuid != mysql_uuid:
            LOG.error("Cluster uuid is differs from local one.")
            LOG.debug("Cluster uuid: %s Local uuid: %s",
                      cluster_uuid, mysql_uuid)
            return 503

        # Node states check.
        state = int(wsrep_data['wsrep_local_state'])
        state_comment = wsrep_data['wsrep_local_state_comment']

        if (state == SYNCED_STATE or
           (state == DONOR_DESYNCED_STATE and args.allow_desynced)):
            WAS_JOINED = True
            LOG.info("State OK: %s", state_comment)
            self.etcd_register_in_path('nodes')
            return 200
        elif state == JOINED_STATE and WAS_JOINED:
            # Node was in the JOINED_STATE in prev check too. Seems to it can't
            # start syncing.
            if OLD_STATE == JOINED_STATE:
                LOG.error("State BAD: %s", state_comment)
                LOG.error("Joined, but not syncyng")
                self._etcd_delete()
                return 503
            else:
                LOG.info("State OK: %s", state_comment)
                LOG.info("Probably will sync soon")
                self.etcd_register_in_path('nodes')
                return 200
        else:
            LOG.info("State OK: %s", state_comment)
            LOG.info("Just joined")
            WAS_JOINED = True
            self.etcd_register_in_path('nodes')
            return 200
        OLD_STATE = state
        LOG.warning("Unknown state: %s", state_comment)
        return 200

    @retry
    def _etcd_delete(self):

        # Add locking before delete operation. Possible race.
        key = os.path.join(ETCD_PATH, 'nodes', IPADDR)
        self.etcd_client.delete(key, recursive=True, dir=True)
        LOG.warning("Deleted node's key '%s'", key)

    @retry
    def _etcd_set(self, data):

        key = os.path.join(ETCD_PATH, data[0], IPADDR)
        self.etcd_client.set(key, data[1], self.ttl)
        LOG.info("Set %s with value '%s'", key, data[1])

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


class GaleraHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):

        LOG.debug("Started processing GET request")
        try:
            checker = GaleraChecker()
            state = checker.check_galera_state()
            self.send_response(state)
            self.end_headers()
        except Exception as err:
            LOG.exception(err)
            self.send_response(503)
            self.end_headers()
        finally:
            LOG.debug("Finished processing GET request")


def run_http(port=8080):
    server_class = BaseHTTPServer.HTTPServer
    handler_class = GaleraHttpHandler
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    LOG.info('Starting http server...')
    httpd.serve_forever()


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
    ETCD_HOST = "etcd.%s" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--allow_desynced", dest='allow_desynced',
                        action='store_true')
    args = parser.parse_args()

    get_config()
    set_globals()
    run_http()
