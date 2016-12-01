#!/usr/bin/env python

import functools
import json
import logging
import os
import os.path
import shutil
import socket
import subprocess
import sys
import time

import etcd
import pymysql.cursors

HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)
DATADIR = "/var/lib/mysql"
INIT_FILE = os.path.join(DATADIR, 'init.ok')
GRASTATE_FILE = os.path.join(DATADIR, 'grastate.dat')
GLOBALS_PATH = '/etc/ccp/globals/globals.json'
EXPECTED_NODES = 3

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

MYSQL_ROOT_PASSWORD = None
CLUSTER_NAME = None
XTRABACKUP_PASSWORD = None
MONITOR_PASSWORD = None
CONNECTION_ATTEMPTS = None
CONNECTION_DELAY = None
ETCD_PATH = None
ETCD_HOST = None
ETCD_PORT = None


class ProcessException(Exception):
    def __init__(self, exit_code):
        self.exit_code = exit_code
        self.msg = "Command exited with code %d" % self.exit_code
        super(ProcessException, self).__init__(self.msg)


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
    for key in ['percona', 'db', 'etcd', 'namespace']:
        variables[key] = global_conf[key]
    LOG.debug(variables)
    return variables


def set_globals():

    config = get_config()
    global MYSQL_ROOT_PASSWORD, CLUSTER_NAME, XTRABACKUP_PASSWORD
    global MONITOR_PASSWORD, CONNECTION_ATTEMPTS, CONNECTION_DELAY
    global ETCD_PATH, ETCD_HOST, ETCD_PORT

    MYSQL_ROOT_PASSWORD = config['db']['root_password']
    CLUSTER_NAME = config['percona']['cluster_name']
    XTRABACKUP_PASSWORD = config['percona']['xtrabackup_password']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    CONNECTION_ATTEMPTS = config['etcd']['connection_attempts']
    CONNECTION_DELAY = config['etcd']['connection_delay']
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    ETCD_HOST = "etcd.%s" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


def get_mysql_client(insecure=False):

    if insecure:
        return pymysql.connect(unix_socket='/var/run/mysqld/mysqld.sock',
                               user='root',
                               connect_timeout=1,
                               read_timeout=1,
                               cursorclass=pymysql.cursors.DictCursor)
    else:
        return pymysql.connect(host='localhost',
                               user='root',
                               password=MYSQL_ROOT_PASSWORD,
                               connect_timeout=1,
                               read_timeout=1,
                               cursorclass=pymysql.cursors.DictCursor)


def get_etcd_client():

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       read_timeout=2)


def datadir_cleanup(path):

    for root, dirs, files in os.walk(path, topdown=False):
        for f in files:
            to_delete = os.path.join(root, f)
            os.remove(to_delete)
        for dir in dirs:
            shutil.rmtree(os.path.join(root, dir))


def create_init_flag():

    if not os.path.isfile(INIT_FILE):
        open(INIT_FILE, 'a').close()
        LOG.debug("Create init_ok file: %s", INIT_FILE)


def execute_cmd(cmd):

    LOG.debug("Executing cmd:\n%s", (str(cmd)))
    kwargs = {
        "shell": True,
        "stdin": sys.stdin,
        "stdout": sys.stdout,
        "stderr": sys.stderr}
    return subprocess.Popen(str(cmd), **kwargs)


def run_cmd(cmd):

    proc = execute_cmd(cmd)
    proc.communicate()
    if proc.returncode != 0:
        raise ProcessException(proc.returncode)


def run_mysqld(available_nodes):

    proc = execute_cmd("mysqld --user=mysql --wsrep_cluster_name='%s'"
                       " --wsrep_cluster_address='gcomm://%s'"
                       " --wsrep_sst_method=xtrabackup-v2"
                       " --wsrep_sst_auth='xtrabackup:%s'"
                       " --wsrep_node_address='%s'"
                       " --pxc_strict_mode=PERMISSIVE" %
                       (CLUSTER_NAME, available_nodes,
                        XTRABACKUP_PASSWORD, IPADDR))
    time.sleep(5)
    if proc.poll() is None:
        LOG.info("Mysqld is started")
        return proc
    proc.communicate()
    raise RuntimeError("Process exited with code: %d" % proc.returncode)


def mysql_exec(mysql_client, sql_list):

    with mysql_client.cursor() as cursor:
        for sql in sql_list:
            LOG.debug("Executing mysql cmd:\n%s", (sql))
            cursor.execute(sql)
        return cursor.fetchall()


@retry
def fetch_status(etcd_client, path):

    key = os.path.join(ETCD_PATH, path)
    try:
        root = etcd_client.get(key)
        result = [str(child.key).replace(key + "/", '')
                  for child in root.children
                  if str(child.key) != key]
        LOG.info("Current nodes in %s is: %s", key, result)
        return result
    except etcd.EtcdKeyNotFound:
        LOG.info("Current nodes in %s is: %s", key, None)
        return []


def fetch_wsrep_data():

    wsrep_data = {}
    mysql_client = get_mysql_client()
    data = mysql_exec(mysql_client, ["SHOW STATUS LIKE 'wsrep%'"])
    for i in data:
        wsrep_data[i['Variable_name']] = i['Value']
    return wsrep_data


@retry
def get_oldest_node_by_ctime(etcd_client, path):

    key = os.path.join(ETCD_PATH, path)
    root = etcd_client.get(key)
    result = [(str(child.key).replace(key + "/", ''), float(child.value))
              for child in root.children]
    result.sort(key=lambda x: x[1])
    LOG.info("Oldest node is %s, am %s", result[0][0], IPADDR)
    return result[0][0]


@retry
def get_oldest_node_by_seqno(etcd_client, path):

    key = os.path.join(ETCD_PATH, path)
    root = etcd_client.get(key)
    result = [(str(child.key).replace(key + "/", ''), int(child.value))
              for child in root.children]
    result.sort(key=lambda x: x[1])
    LOG.debug("ALL seqno is %s", result)
    LOG.info("Oldest node is %s, am %s", result[-1][0], IPADDR)
    return result[-1][0]


@retry
def _etcd_set(etcd_client, data, ttl):

    key = os.path.join(ETCD_PATH, data[0])
    etcd_client.set(key, data[1], ttl=ttl)
    LOG.info("Set %s with value '%s'", key, data[1])


def etcd_register_in_path(etcd_client, path, ttl=60):

    key = os.path.join(path, IPADDR)
    _etcd_set(etcd_client, (key, time.time()), ttl)


def etcd_set_seqno(etcd_client, ttl):

    seqno = mysql_get_seqno()
    key = os.path.join('seqno', IPADDR)
    _etcd_set(etcd_client, (key, seqno), ttl)


def etcd_deregister_in_path(etcd_client, path):

    key = os.path.join(ETCD_PATH, path, IPADDR)
    try:
        etcd_client.delete(key, recursive=True)
        LOG.warning("Deleted key %s", key)
    except etcd.EtcdKeyNotFound:
        LOG.warning("Key %s not exist", key)


def mysql_get_seqno():

    if os.path.isfile(GRASTATE_FILE):
        with open(GRASTATE_FILE) as f:
            content = f.readlines()
        for line in content:
            if line.startswith('seqno'):
                return line.partition(':')[2].strip()


def get_cluster_state(etcd_client):

    key = os.path.join(ETCD_PATH, 'state')
    try:
        state = etcd_client.read(key).value
        return state
    except etcd.EtcdKeyNotFound:
        return None


def wait_for_expected_state(etcd_client, ttl):

    while True:
        status = fetch_status(etcd_client, 'queue')
        if len(status) != EXPECTED_NODES:
            LOG.debug("Current number of nodes is %s, expected: %s, sleeping",
                      len(status), EXPECTED_NODES)
            time.sleep(1)
        else:
            wait_for_my_turn(etcd_client)
            break


def wait_for_my_turn(etcd_client):

    state = get_cluster_state(etcd_client)
    LOG.info("Waiting for my turn to join cluster")
    while True:
        if state == "RECOVERY":
            oldest_node = get_oldest_node_by_seqno(etcd_client, 'seqno')
        else:
            oldest_node = get_oldest_node_by_ctime(etcd_client, 'queue')
        if IPADDR == oldest_node:
            LOG.info("It's my turn to join the cluster")
            return
        else:
            time.sleep(5)


def wait_for_sync(mysqld):

    while True:
        try:
            wsrep_data = fetch_wsrep_data()
            state = int(wsrep_data['wsrep_local_state'])
            if state == 4:
                LOG.info("Node synced")
                # If sync was done by SST all files in datadir was lost
                create_init_flag()
                break
            else:
                LOG.debug("Waiting node to be synced. Current state is: %s",
                          wsrep_data['wsrep_local_state_comment'])
                time.sleep(5)
        except Exception:
            if mysqld.poll() is None:
                time.sleep(5)
            else:
                LOG.error('Mysqld was terminated, exit code was: %s',
                          mysqld.returncode)
                sys.exit(mysqld.returncode)


def check_if_im_last(etcd_client):

    queue_status = fetch_status(etcd_client, 'queue')
    nodes_status = fetch_status(etcd_client, 'nodes')
    if not queue_status and len(nodes_status) == EXPECTED_NODES:
        LOG.info("Looks like this node is the last one")
        return True
    else:
        LOG.info("I'm not the last node")
        return False


def create_join_list(status):

    if IPADDR in status:
        status.remove(IPADDR)
    if not status:
        LOG.info("No available nodes found. Assuming I'm first")
        return ""
    else:
        LOG.info("Joining to nodes %s", ','.join(status))
        return ','.join(status)


def update_uuid(etcd_client):

    wsrep_data = fetch_wsrep_data()
    uuid = wsrep_data['wsrep_cluster_state_uuid']
    _etcd_set(etcd_client, ('uuid', uuid), ttl=None)


def update_cluster_state(etcd_client, state):

    _etcd_set(etcd_client, ('state', state), ttl=None)


def wait_for_mysqld(proc):

    code = proc.wait()
    LOG.info("Process exited with code %d", code)
    sys.exit(code)


def mysql_init():
    LOG.info("Init file '%s' not found, doing full init", INIT_FILE)
    datadir_cleanup(DATADIR)
    run_cmd("mysqld --initialize-insecure")
    mysqld = execute_cmd("mysqld --skip-networking")

    LOG.info("Waiting mysql to start...")
    for i in range(0, 29):
        try:
            mysql_client = get_mysql_client(insecure=True)
            mysql_exec(mysql_client, ['SELECT 1'])
            break
        except Exception as err:
            time.sleep(1)
    else:
        LOG.info("Mysql boot failed")
        sys.exit(1)

    LOG.info("Mysql is running, setting up the permissions")
    sql_list = ["CREATE USER 'root'@'%%' IDENTIFIED BY '%s'" %
                MYSQL_ROOT_PASSWORD,
                "GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION",
                "ALTER USER 'root'@'localhost' IDENTIFIED BY '%s'" %
                MYSQL_ROOT_PASSWORD,
                "CREATE USER 'xtrabackup'@'localhost' IDENTIFIED BY '%s'" %
                XTRABACKUP_PASSWORD,
                "GRANT RELOAD,PROCESS,LOCK TABLES,REPLICATION CLIENT ON *.* TO"
                " 'xtrabackup'@'localhost'",
                "GRANT REPLICATION CLIENT ON *.* TO monitor@'%%' IDENTIFIED BY"
                " '%s'" % MONITOR_PASSWORD,
                "DROP DATABASE IF EXISTS test",
                "FLUSH PRIVILEGES"]
    try:
        mysql_exec(mysql_client, sql_list)
    except Exception as err:
        LOG.exception(err)

    run_cmd("pkill mysqld")
    for i in range(0, 29):
        if mysqld.poll() is None:
            time.sleep(1)
        else:
            break
    else:
        LOG.info("Can't kill the mysqld process used for bootstraping")
        sys.exit(1)
    create_init_flag()
    LOG.info("Mysql bootstraping is done")


def check_cluster(etcd_client):

    state = get_cluster_state(etcd_client)
    nodes_status = fetch_status(etcd_client, 'nodes')
    if not nodes_status and state == 'STEADY':
        LOG.warning("Cluster is in the STEADY state, but there no"
                    " alive nodes detected, running cluster recovery")
        update_cluster_state(etcd_client, 'RECOVERY')

def run_step_one(etcd_client, lock, ttl):

    LOG.info("Step 1 - Queue")
    state = get_cluster_state(etcd_client)
    etcd_register_in_path(etcd_client, 'queue', ttl)
    if state == "RECOVERY":
        etcd_set_seqno(etcd_client, ttl=None)
    lock.release()
    LOG.info("Successfuly released lock")
    wait_for_expected_state(etcd_client, ttl)


def run_step_two(etcd_client, lock, ttl):

    LOG.info("Step 2 - Joining the cluster")
    LOG.info("Locking...")
    lock.acquire(blocking=True, lock_ttl=ttl)
    LOG.info("Successfuly acquired lock")
    state = get_cluster_state(etcd_client)
    nodes_status = fetch_status(etcd_client, 'nodes')
    first_one = False if nodes_status else True
    available_nodes = create_join_list(nodes_status)
    mysqld = run_mysqld(available_nodes)
    wait_for_sync(mysqld)
    etcd_register_in_path(etcd_client, 'nodes', ttl)
    etcd_deregister_in_path(etcd_client, 'queue')
    if state == "RECOVERY":
        etcd_deregister_in_path(etcd_client, 'seqno')
    last_one = check_if_im_last(etcd_client)
    lock.release()
    LOG.info("Successfuly released lock")
    return (first_one, last_one, mysqld)


def run_step_tree(etcd_client, first_one, last_one):
    LOG.info("Step 3 - Update cluster metadata")
    state = get_cluster_state(etcd_client)
    if first_one:
        update_uuid(etcd_client)
        if state != 'RECOVERY':
            update_cluster_state(etcd_client, 'BUILDING')
    if last_one:
        update_cluster_state(etcd_client, 'STEADY')


def main(ttl):

    if not os.path.isfile(INIT_FILE):
        mysql_init()
    else:
        run_cmd("mysqld_safe --wsrep-recover")
        LOG.info("Init file '%s' found. Skiping mysql bootstrap", INIT_FILE)

    try:
        etcd_client = get_etcd_client()
        lock = etcd.Lock(etcd_client, 'galera')
        LOG.info("Locking...")
        lock.acquire(blocking=True, lock_ttl=ttl)
        LOG.info("Successfuly acquired lock")
        check_cluster(etcd_client)
        state = get_cluster_state(etcd_client)

        # Scenario 1: Initial bootstrap
        if state is None or state == 'BUILDING':
            LOG.info("No running cluster detected - starting bootstrap")
            run_step_one(etcd_client, lock, ttl)
            first_one, last_one, mysqld = run_step_two(etcd_client, lock, ttl)
            run_step_tree(etcd_client, first_one, last_one)
            LOG.info("Bootsraping is done. Node is ready.")

        # Scenario 2: Re-connect
        elif state == 'STEADY':
            LOG.info("Detected running cluster, re-connecting")
            nodes_status = fetch_status(etcd_client, 'nodes')
            available_nodes = create_join_list(nodes_status)
            mysqld = run_mysqld(available_nodes)
            wait_for_sync(mysqld)
            etcd_register_in_path(etcd_client, 'nodes', ttl)
            LOG.info("Node joined and ready")
            lock.release()
            LOG.info("Successfuly released lock")

        # Scenario 3: Recovery
        elif state == 'RECOVERY':
            LOG.warning("Cluster is in the RECOVERY state, re-connecting to"
                        " the node with the oldest data")
            run_step_one(etcd_client, lock, ttl)
            first_one, last_one, mysqld = run_step_two(etcd_client, lock, ttl)
            run_step_tree(etcd_client, first_one, last_one)
            LOG.info("Recovery is done. Node is ready.")

        wait_for_mysqld(mysqld)
    except Exception as err:
        LOG.exception(err)
    finally:
        etcd_deregister_in_path(etcd_client, 'queue')
        etcd_deregister_in_path(etcd_client, 'nodes')
        etcd_deregister_in_path(etcd_client, 'queue')
        lock.release()
        LOG.info("Successfuly released lock")


if __name__ == "__main__":
    get_config()
    set_globals()
    main(ttl=300)

# vim: set ts=4 sw=4 tw=0 et :
