#!/usr/bin/env python

import argparse
import json
import functools
import logging
import os
import socket
import sys
import time
import BaseHTTPServer
import SocketServer

import etcd
import pymysql.cursors

parser = argparse.ArgumentParser()
parser.add_argument("--allow_desynced", dest='allow_desynced',
                    action='store_true')
args = parser.parse_args()

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

class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def init(self):
        self.ttl = 30
        self.get_etcd_client()
        self.get_mysql_client()


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


    def get_etcd_client(self):

        self.etcd_client = etcd.Client(host=ETCD_HOST,
                                       port=ETCD_PORT,
                                       allow_reconnect=True,
                                       read_timeout=2)


    @retry
    def get_mysql_client(self):
        self.mysql_client = pymysql.connect(host='127.0.0.1',
                                            port=3306,
                                            user='monitor',
                                            password=MONITOR_PASSWORD,
                                            connect_timeout=1,
                                            read_timeout=1,
                                            cursorclass=pymysql.cursors.DictCursor)

    @retry
    def fetch_mysql_data(self):

        with self.mysql_client.cursor() as cursor:
            sql = "SHOW STATUS LIKE 'wsrep%'"
            cursor.execute(sql)
            data = cursor.fetchall()
            return data


    def check_galera_state(self):

        global WAS_JOINED
        global OLD_STATE
        wsrep_data = {}
        status = 'Unknown'

        data = self.fetch_mysql_data()
        for i in data:
            wsrep_data[i['Variable_name']] = i['Value']

        state = int(wsrep_data['wsrep_local_state'])
        state_comment = wsrep_data['wsrep_local_state_comment']

        if state == SYNCED_STATE or (state == DONOR_DESYNCED_STATE and args.allow_desynced):
            WAS_JOINED = True
            LOG.info("State OK: %s", state_comment)
            self.etcd_register_in_path('nodes')
            self.send_response(200)
        elif state == JOINED_STATE and WAS_JOINED:
            if OLD_STATE == JOINED_STATE:
                LOG.info("State BAD: %s", state_comment)
                LOG.info("Joined, but not syncyng")
                self._etcd_delete()
                self.send_response(503)
            else:
                LOG.info("State OK: %s", state_comment)
                LOG.info("Probably will sync soon")
                self.etcd_register_in_path('nodes')
                self.send_response(200)
        else:
            LOG.info("State OK: %s", state_comment)
            LOG.info("Just joined")
            WAS_JOINED = True
            self.etcd_register_in_path('nodes')
            self.send_response(200)
        OLD_STATE = state


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


    def etcd_register_in_path(self, path):

        key = os.path.join(ETCD_PATH, path, IPADDR)
        self._etcd_set((path, time.time()))

    def do_GET(self):

        LOG.debug("Start time: %s", time.time())
        self.log_request()
        #self.log_error()
        try:
            self.init()
            self.check_galera_state()
        except Exception as err:
            LOG.exception(err)
            self._etcd_delete()
            self.send_response(503)
        finally:
            LOG.debug("Finish time: %s", time.time())


def run_http(port=8080):
    server_class = BaseHTTPServer.HTTPServer
    handler_class = HttpHandler
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

    get_config()
    set_globals()
    run_http()

#    while True:
#        try:
#            mysql_client = get_mysql_client()
#            etcd_client = get_etcd_client()
#            check_galera_state(mysql_client, etcd_client, ttl=30)
#        except Exception as err:
#            LOG.exception(err)
#            etcd_client = get_etcd_client()
#            _etcd_delete(etcd_client)
#            # Return 500
#        LOG.info("Sleeping for 5 sec")
#        time.sleep(5)
