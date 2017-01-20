#!/usr/bin/env python

import argparse
import functools
import json
import logging
import os
import socket
import subprocess
import sys
import time

import etcd

HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)
BACKEND_NAME = "galera-cluster"
SERVER_NAME = "primary"
GLOBALS_PATH = '/etc/ccp/globals/globals.json'

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

CONNECTION_ATTEMPTS = None
CONNECTION_DELAY = None
ETCD_PATH = None
ETCD_HOST = None
ETCD_PORT = None

# Haproxy constant for health checks
SRV_STATE_RUNNING = 2
SRV_CHK_RES_PASSED = 3


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
    for key in ['percona', 'etcd', 'namespace', 'cluster_domain']:
        variables[key] = global_conf[key]
    LOG.debug(variables)
    return variables


def set_globals():

    config = get_config()
    global CONNECTION_ATTEMPTS, CONNECTION_DELAY
    global ETCD_PATH, ETCD_HOST, ETCD_PORT

    CONNECTION_ATTEMPTS = config['etcd']['connection_attempts']
    CONNECTION_DELAY = config['etcd']['connection_delay']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s.svc.%s" % (config['namespace'],
                                    config['cluster_domain'])
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


def get_etcd_client():

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       read_timeout=2)


def get_socket():
    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_socket.settimeout(5)
    unix_socket.connect('/var/run/haproxy/admin.sock')
    return unix_socket


def run_haproxy():

    cmd = ["haproxy", "-f", "/etc/haproxy/haproxy.conf"]
    LOG.info("Executing cmd:\n%s", cmd)
    proc = subprocess.Popen(cmd)
    return proc


def check_haproxy(proc):

    ret_code = proc.poll()
    if ret_code is not None:
        LOG.error("Haproxy was terminated, exit code was: %s",
                  proc.returncode)
        sys.exit(proc.returncode)


@retry
def etcd_set(etcd_client, key, value, ttl, dir=False, append=False, **kwargs):

    etcd_client.write(key, value, ttl, dir, append, **kwargs)
    LOG.info("Set %s with value '%s'", key, value)


@retry
def etcd_refresh(etcd_client, path, ttl):

    key = os.path.join(ETCD_PATH, path)
    etcd_client.refresh(key, ttl)
    LOG.info("Refreshed %s ttl. New ttl is '%s'", key, ttl)


def send_command(cmd):

    LOG.debug("Sending '%s' cmd to haproxy", cmd)
    sock = get_socket()
    sock.send(cmd + '\n')
    file_handle = sock.makefile()
    data = file_handle.read().splitlines()
    sock.close()
    return data


def get_haproxy_status():

    state_data = send_command("show servers state galera-cluster")
    stat_data = send_command("show stat typed")
    # we need to parse string which looks like this:
    # 'S.2.1.73.addr.1:CGS:str:10.233.76.104:33306'
    for line in stat_data:
        if "addr" in line:
            ip, port = line.split(':')[-2:]
    # It returns as a 3 elements list, with string inside.
    # We have to do some magic, to make a valid dict out of it.
    keys = state_data[1].split(' ')
    keys.pop(0)
    values = state_data[2].split(' ')
    data_dict = dict(zip(keys, values))
    data_dict['backend'] = "%s:%s" % (ip, port)
    return data_dict


def get_cluster_state(etcd_client):

    key = os.path.join(ETCD_PATH, 'state')
    try:
        state = etcd_client.read(key).value
        return state
    except etcd.EtcdKeyNotFound:
        return None


def wait_for_cluster_to_be_steady(etcd_client, haproxy_proc):

    while True:
        state = get_cluster_state(etcd_client)
        if state != 'STEADY':
            check_haproxy(haproxy_proc)
            LOG.warning("Cluster is not in the STEADY state, waiting...")
            time.sleep(5)
        else:
            break


def set_server_addr(leader_ip):

    cmds = ["set server %s/%s addr %s port 33306" % (
            BACKEND_NAME, SERVER_NAME, leader_ip),
            "set server %s/%s check-port 33306" % (
            BACKEND_NAME, SERVER_NAME)]
    for cmd in cmds:
        # Bug in haproxy. Sometimes, haproxy can't convert port str to int.
        # Will be fixed in 1.7.2
        while True:
            response = send_command(cmd)
            if "problem converting port" in response[0]:
                LOG.error("Port convertation failed, trying again...")
                time.sleep(1)
            else:
                LOG.info("Successfuly set backend to %s:33306", leader_ip)
                return


def get_leader(etcd_client):

    key = os.path.join(ETCD_PATH, 'leader')
    try:
        leader = etcd_client.read(key).value
    except etcd.EtcdKeyNotFound:
        leader = None

    LOG.info("Current leader is: %s", leader)
    return leader


def set_leader(etcd_client, ttl, **kwargs):

    key = os.path.join(ETCD_PATH, 'leader')
    etcd_set(etcd_client, key, IPADDR, ttl, **kwargs)


def refresh_leader(etcd_client, ttl):

    key = os.path.join(ETCD_PATH, 'leader')
    etcd_refresh(etcd_client, key, ttl)


def do_we_need_to_reconfigure_haproxy(leader):

    haproxy_stat = get_haproxy_status()
    haproxy_leader = haproxy_stat['backend']
    leader += ":33306"
    LOG.debug("Haproxy server is: %s. Current leader is: %s",
              haproxy_leader, leader)
    return haproxy_leader != leader


def run_daemon(ttl):

    LOG.debug("My IP is: %s", IPADDR)
    haproxy_proc = run_haproxy()
    etcd_client = get_etcd_client()
    while True:
        wait_for_cluster_to_be_steady(etcd_client, haproxy_proc)
        leader = get_leader(etcd_client)
        if not leader:
            set_leader(etcd_client, ttl, prevExist=False)
            leader = IPADDR
        elif leader == IPADDR:
            refresh_leader(etcd_client, ttl)

        if do_we_need_to_reconfigure_haproxy(leader):
            LOG.info("Updating haproxy configuration")
            set_server_addr(leader)
        check_haproxy(haproxy_proc)
        LOG.info("Sleeping for 5 sec...")
        time.sleep(5)


def run_readiness():

    etcd_client = get_etcd_client()
    state = get_cluster_state(etcd_client)
    if state != 'STEADY':
        LOG.error("Cluster is not in the STEADY state")
        sys.exit(1)
    leader = get_leader(etcd_client)
    if not leader:
        LOG.error("No leader found")
        sys.exit(1)
    else:
        if do_we_need_to_reconfigure_haproxy(leader):
            LOG.error("Haproxy configuration is wrong")
            sys.exit(1)
    haproxy_stat = get_haproxy_status()
    LOG.debug(haproxy_stat)
    if (int(haproxy_stat['srv_op_state']) != SRV_STATE_RUNNING and
            int(haproxy_stat['srv_check_result']) != SRV_CHK_RES_PASSED):
        LOG.error("Current leader is not alive")
        sys.exit(1)
    LOG.info("Service is ready")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('type', choices=['daemon', 'readiness'])
    args = parser.parse_args()

    get_config()
    set_globals()
    if args.type == 'daemon':
        run_daemon(ttl=20)
    elif args.type == 'readiness':
        run_readiness()

# vim: set ts=4 sw=4 tw=0 et :
