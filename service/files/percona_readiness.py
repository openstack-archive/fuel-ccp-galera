#!/usr/bin/env python

import json
import functools
import os.path
import sys
import time

import etcd
import pymysql.cursors

GLOBALS_PATH = '/etc/ccp/globals/globals.json'


def get_config():

    variables = {}
    with open(GLOBALS_PATH) as f:
        global_conf = json.load(f)
    for key in ['percona', 'etcd', 'namespace']:
        variables[key] = global_conf[key]
    return variables


def set_globals():

    config = get_config()
    global CONNECTION_ATTEMPTS, CONNECTION_DELAY, MONITOR_PASSWORD
    global ETCD_PATH, ETCD_HOST, ETCD_PORT, CLUSTER_NAME

    CLUSTER_NAME = config['percona']['cluster_name']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    CONNECTION_ATTEMPTS = config['etcd']['connection_attempts']
    CONNECTION_DELAY = config['etcd']['connection_delay']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])
def get_mysql_client():

    return pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='monitor',
                           password=MONITOR_PASSWORD,
                           connect_timeout=1,
                           read_timeout=1,
                           cursorclass=pymysql.cursors.DictCursor)


def get_etcd_client():

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       read_timeout=2)


def _etcd_read(etcd_client, path):

    key = os.path.join(ETCD_PATH, path)
    return etcd_client.read(key).value


def check_cluster_state():

    etcd_client = get_etcd_client()
    state = _etcd_read(etcd_client, 'state')
    if state != 'STEADY':
        sys.exit(1)

def get_cluster_uuid():

    etcd_client = get_etcd_client()
    return _etcd_read(etcd_client, 'uuid')


def fetch_wsrep_data():

    data = {}
    try:
        mysql_client = get_mysql_client()
        with mysql_client.cursor() as cursor:
            sql = "SHOW STATUS LIKE 'wsrep%'"
            cursor.execute(sql)
            for i in cursor.fetchall():
                data[i['Variable_name']] = i['Value']
            return data
    except Exception:
        sys.exit(1)


def check_galera():

    results = {}
    wsrep_data = fetch_wsrep_data()
    uuid = get_cluster_uuid()

    if (wsrep_data["wsrep_local_state_comment"] != "Synced" or
            wsrep_data["wsrep_evs_state"] != "OPERATIONAL" or
            wsrep_data["wsrep_connected"] != "ON" or
            wsrep_data["wsrep_ready"] != "ON" or
            wsrep_data["wsrep_cluster_state_uuid"] != uuid):

        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    get_config()
    set_globals()
    check_cluster_state()
    check_galera()
