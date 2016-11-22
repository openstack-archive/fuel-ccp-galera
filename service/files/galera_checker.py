#!/usr/bin/env python

import argparse
import json
import functools
import logging
import os
import socket
import sys
import time

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


def retry(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = CONNECTION_ATTEMPTS
        delay = CONNECTION_DELAY
        while attempts > 1:
            try:
                return f(*args, **kwargs)
            except etcd.EtcdException as e:
                LOG.warning('Etcd is not ready: %s', str(e))
                LOG.warning('Retrying in %d seconds...', delay)
                time.sleep(delay)
                attempts -= 1
        return f(*args, **kwargs)
    return wrap


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
    global CONNECTION_ATTEMPTS, CONNECTION_DELAY
    global ETCD_PATH, ETCD_HOST, ETCD_PORT

    CLUSTER_NAME = config['percona']['cluster_name']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    CONNECTION_ATTEMPTS = config['etcd']['connection_attempts']
    CONNECTION_DELAY = config['etcd']['connection_delay']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


def get_etcd_client():

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       read_timeout=2)


def get_mysql_client():
    return pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='monitor',
                           password=MONITOR_PASSWORD,
                           connect_timeout=1,
                           read_timeout=1,
                           cursorclass=pymysql.cursors.DictCursor)


def fetch_mysql_data(mysql_client):

    with mysql_client.cursor() as cursor:
        sql = "SHOW STATUS LIKE 'wsrep%'"
        cursor.execute(sql)
        data = cursor.fetchall()
        return data


def check_galera_state(mysql_client, etcd_client, ttl):

    global WAS_JOINED
    global OLD_STATE
    wsrep_data = {}
    status = 'Unknown'

    data = fetch_mysql_data(mysql_client)
    for i in data:
        wsrep_data[i['Variable_name']] = i['Value']

    state = int(wsrep_data['wsrep_local_state'])
    state_comment = wsrep_data['wsrep_local_state_comment']

    if state == SYNCED_STATE:
        WAS_JOINED = True
        LOG.info("State OK: %s", state_comment)
        etcd_register_in_path(etcd_client, 'nodes', ttl)
    elif state == DONOR_DESYNCED_STATE and args.allow_desynced:
        state = wsrep_data['wsrep_local_state_comment']
        WAS_JOINED = True
        LOG.info("State OK: %s", state_comment)
        etcd_register_in_path(etcd_client, 'nodes', ttl)
    elif state == JOINED_STATE and WAS_JOINED:
        if OLD_STATE == JOINED_STATE:
            LOG.info("State BAD: %s", state_comment)
            LOG.info("Joined, but not syncyng")
            _etcd_delete(etcd_client)
            # Return 500
            WAS_JOINED = True
        else:
            LOG.info("State OK: %s", state_comment)
            LOG.info("Probably will sync soon")
            etcd_register_in_path(etcd_client, 'nodes', ttl)
    else:
        LOG.info("State OK: %s", state_comment)
        LOG.info("Just joined")
        WAS_JOINED = True
        etcd_register_in_path(etcd_client, 'nodes', ttl)
    OLD_STATE = state


@retry
def _etcd_delete(etcd_client):

    # Add locking before delete operation. Possible race.
    key = os.path.join(ETCD_PATH, 'nodes', IPADDR)
    # etcd_client.delete(key, recursive=True, dir=True)
    LOG.warning("NOOP: Deleted node's key '%s'", key)


def _etcd_set(etcd_client, data, ttl):

    key = os.path.join(ETCD_PATH, data[0], IPADDR)
    etcd_client.set(key, data[1], ttl=ttl)
    LOG.info("Set %s with value '%s'", key, data[1])


@retry
def etcd_register_in_path(etcd_client, path, ttl):

    key = os.path.join(ETCD_PATH, path, IPADDR)
    _etcd_set(etcd_client, (path, time.time()), ttl)


if __name__ == "__main__":

    get_config()
    set_globals()
    LOG.info("Sleeping for 40 sec while cluster bootstraping")
    time.sleep(40)

    while True:
        try:
            mysql_client = get_mysql_client()
            etcd_client = get_etcd_client()
            check_galera_state(mysql_client, etcd_client, ttl=30)
        except Exception as err:
            LOG.exception(err)
            etcd_client = get_etcd_client()
            _etcd_delete(etcd_client)
            # Return 500
        LOG.info("Sleeping for 5 sec")
        time.sleep(5)
