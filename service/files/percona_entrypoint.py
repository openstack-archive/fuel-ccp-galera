#!/usr/bin/env python

import fileinput
import logging
import os
import os.path
import shutil
import socket
import subprocess
import signal
import six.moves
import sys
import time

import etcd

from entrypoint_utils import retry
from entrypoint_utils import get_config
from entrypoint_utils import get_etcd_client
from entrypoint_utils import get_mysql_client
from entrypoint_utils import etcd_set
from entrypoint_utils import etcd_get
from entrypoint_utils import etcd_register_in_path
from entrypoint_utils import etcd_deregister_in_path

HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)
DATADIR = "/var/lib/mysql"
INIT_FILE = os.path.join(DATADIR, 'init.ok')
PID_FILE = os.path.join(DATADIR, "mysqld.pid")
GRASTATE_FILE = os.path.join(DATADIR, 'grastate.dat')
GLOBALS_PATH = '/etc/ccp/globals/globals.json'

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

MYSQL_SOCK = '/var/run/mysqld/mysqld.sock'
FORCE_BOOTSTRAP = None
FORCE_BOOTSTRAP_NODE = None
EXPECTED_NODES = None
MYSQL_ROOT_PASSWORD = None
CLUSTER_NAME = None
XTRABACKUP_PASSWORD = None
MONITOR_PASSWORD = None
CONNECTION_ATTEMPTS = None
CONNECTION_DELAY = None
ETCD_PATH = None
ETCD_HOSTS = None


class ProcessException(Exception):
    def __init__(self, exit_code):
        self.exit_code = exit_code
        self.msg = "Command exited with code %d" % self.exit_code
        super(ProcessException, self).__init__(self.msg)


def set_globals(config):

    global MYSQL_ROOT_PASSWORD, CLUSTER_NAME, XTRABACKUP_PASSWORD
    global MONITOR_PASSWORD, CONNECTION_ATTEMPTS, CONNECTION_DELAY
    global ETCD_PATH, ETCD_HOSTS, EXPECTED_NODES
    global FORCE_BOOTSTRAP, FORCE_BOOTSTRAP_NODE

    FORCE_BOOTSTRAP = config['percona']['force_bootstrap']['enabled']
    FORCE_BOOTSTRAP_NODE = config['percona']['force_bootstrap']['node']
    MYSQL_ROOT_PASSWORD = config['db']['root_password']
    CLUSTER_NAME = config['percona']['cluster_name']
    XTRABACKUP_PASSWORD = config['percona']['xtrabackup_password']
    MONITOR_PASSWORD = config['percona']['monitor_password']
    CONNECTION_ATTEMPTS = config['etcd']['connection_attempts']
    CONNECTION_DELAY = config['etcd']['connection_delay']
    EXPECTED_NODES = int(config['percona']['cluster_size'])
    ETCD_PATH = "/galera/%s" % config['percona']['cluster_name']
    etcd_host = "etcd.%s.svc.cluster.local" % config['namespace']
    etcd_port = int(config['etcd']['client_port']['cont'])
    ETCD_HOSTS = ((etcd_host, etcd_port),)


def datadir_cleanup(path):

    for filename in os.listdir(path):
        fullpath = os.path.join(path, filename)
        if os.path.isdir(fullpath):
            shutil.rmtree(fullpath)
        else:
            os.remove(fullpath)


def create_init_flag():

    if not os.path.isfile(INIT_FILE):
        open(INIT_FILE, 'a').close()
        LOG.debug("Create init_ok file: %s", INIT_FILE)
    else:
        LOG.debug("Init file: '%s' already exists", INIT_FILE)


def run_cmd(cmd, check_result=False):

    LOG.debug("Executing cmd:\n%s", cmd)
    proc = subprocess.Popen(cmd, shell=True)
    if check_result:
        proc.communicate()
        if proc.returncode != 0:
            raise ProcessException(proc.returncode)
    return proc


def run_mysqld(available_nodes, etcd_client, lock):

    cmd = ("mysqld --user=mysql --wsrep_cluster_name=%s"
           " --wsrep_cluster_address=%s"
           " --wsrep_sst_method=xtrabackup-v2"
           " --wsrep_sst_auth=%s"
           " --wsrep_node_address=%s"
           " --pxc_strict_mode=PERMISSIVE" %
           (six.moves.shlex_quote(CLUSTER_NAME),
            "gcomm://%s" % six.moves.shlex_quote(available_nodes),
            "xtrabackup:%s" % six.moves.shlex_quote(XTRABACKUP_PASSWORD),
            six.moves.shlex_quote(IPADDR)))
    mysqld_proc = run_cmd(cmd)
    wait_for_mysqld_to_start(mysqld_proc, insecure=False)

    def sig_handler(signum, frame):
        LOG.info("Caught a signal: %d", signum)
        for path in ['seqno', 'queue', 'nodes', 'leader']:
            key = os.path.join(ETCD_PATH, path, IPADDR)
            prevValue = IPADDR if path == 'leader' else False
            etcd_deregister_in_path(etcd_client, key, prevValue=prevValue)
        release_lock(lock)
        mysqld_proc.send_signal(signum)

    signal.signal(signal.SIGTERM, sig_handler)
    return mysqld_proc


def mysql_exec(mysql_client, sql_list):

    with mysql_client.cursor() as cursor:
        for cmd, args in sql_list:
            LOG.debug("Executing mysql cmd: %s\nWith the following args: '%s'",
                      cmd, args)
            cursor.execute(cmd, args)
        return cursor.fetchall()


def fetch_status(etcd_client, path):

    key = os.path.join(ETCD_PATH, path)
    try:
        root = etcd_get(etcd_client, key)
    except etcd.EtcdKeyNotFound:
        LOG.debug("Current nodes in %s is: %s", key, None)
        return []

    result = [str(child.key).replace(key + "/", '')
              for child in root.children
              if str(child.key) != key]
    LOG.debug("Current nodes in %s is: %s", key, result)
    return result


def fetch_wsrep_data():

    wsrep_data = {}
    mysql_client = retry(get_mysql_client, 3)(unix_socket=MYSQL_SOCK,
                                              password=MYSQL_ROOT_PASSWORD)
    data = mysql_exec(mysql_client, [("SHOW STATUS LIKE 'wsrep%'", None)])
    for i in data:
        wsrep_data[i['Variable_name']] = i['Value']
    return wsrep_data


def get_oldest_node_by_seqno(etcd_client, path):

    """
    This fucntion returns IP addr of the node with the highes seqno.

    seqno(sequence number) indicates the number of transactions ran thought
    that node. Node with highes seqno is the node with the lates data.

    """
    key = os.path.join(ETCD_PATH, path)
    root = retry(etcd_get, CONNECTION_ATTEMPTS)(etcd_client, key)
    # We need to cut etcd path prefix like "/galera/k8scluster/seqno/" to get
    # the IP addr of the node.
    prefix = key + "/"
    result = sorted([(str(child.key).replace(prefix, ''), int(child.value))
              for child in root.children])
    result.sort(key=lambda x: x[1])
    LOG.debug("ALL seqno is %s", result)
    LOG.info("Oldest node is %s, am %s", result[-1][0], IPADDR)
    return result[-1][0]


def etcd_set_seqno(etcd_client, ttl):

    seqno = mysql_get_seqno()
    key = os.path.join(ETCD_PATH, 'seqno', IPADDR)
    retry(etcd_set, CONNECTION_ATTEMPTS)(etcd_client, key, seqno, ttl)


def mysql_get_seqno():

    if os.path.isfile(GRASTATE_FILE):
        with open(GRASTATE_FILE) as f:
            content = f.readlines()
        for line in content:
            if line.startswith('seqno'):
                return line.partition(':')[2].strip()
    else:
        LOG.warning("Can't find a '%s' file. Setting seqno to '-1'",
                    GRASTATE_FILE)
        return -1


def get_cluster_state(etcd_client):

    key = os.path.join(ETCD_PATH, 'state')
    try:
        state = etcd_client.read(key).value
        return state
    except etcd.EtcdKeyNotFound:
        return None


def check_for_stale_seqno(etcd_client):

    queue_set = set(fetch_status(etcd_client, 'queue'))
    seqno_set = set(fetch_status(etcd_client, 'seqno'))
    difference = queue_set - seqno_set
    if difference:
        LOG.warning("Found stale seqno entries: %s, deleting", difference)
        for ip in difference:
            key = os.path.join(ETCD_PATH, 'seqno', ip)
            try:
                etcd_client.delete(key)
                LOG.warning("Deleted key %s", key)
            except etcd.EtcdKeyNotFound:
                LOG.warning("Key %s not exist", key)
    else:
        LOG.debug("Found seqno set is equals to the queue set: %s = %s",
                  queue_set, seqno_set)


def wait_for_expected_state(etcd_client, ttl):

    while True:
        status = fetch_status(etcd_client, 'queue')
        if len(status) > EXPECTED_NODES:
            LOG.debug("Current number of nodes is %s, expected: %s, sleeping",
                      len(status), EXPECTED_NODES)
            time.sleep(10)
        elif len(status) < EXPECTED_NODES:
            LOG.debug("Current number of nodes is %s, expected: %s, sleeping",
                      len(status), EXPECTED_NODES)
            time.sleep(1)
        else:
            wait_for_my_turn(etcd_client)
            break

def wait_for_my_seqno(etcd_client):

        oldest_node = get_oldest_node_by_seqno(etcd_client, 'seqno')
        if IPADDR == oldest_node:
            LOG.info("It's my turn to join the cluster")
            return
        else:
            time.sleep(5)

def wait_for_my_turn(etcd_client):

    check_for_stale_seqno(etcd_client)
    LOG.info("Waiting for my turn to join cluster")
    if FORCE_BOOTSTRAP:
        LOG.warning("Force bootstrap flag was detected, skiping normal"
                    " bootstrap procedure")
        if FORCE_BOOTSTRAP_NODE is None:
            LOG.error("Force bootstrap node wasn't set. Can't continue")
            sys.exit(1)

        LOG.debug("Force bootstrap node is %s", FORCE_BOOTSTRAP_NODE)
        my_node_name = os.environ['CCP_NODE_NAME']
        if my_node_name == FORCE_BOOTSTRAP_NODE:
            LOG.info("This node is the force boostrap one.")
            set_safe_to_bootstrap()
            return
        else:
            LOG.info("This node is not the force boostrap one."
                     " Waiting for the bootstrap one to create a cluster.")
            while True:
                nodes = fetch_status(etcd_client, 'nodes')
                if nodes:
                    wait_for_my_seqno(etcd_client)
                    return
                else:
                    time.sleep(5)
    else:
        wait_for_my_seqno(etcd_client)


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

    sleep = 10
    queue_status = fetch_status(etcd_client, 'queue')
    while True:
        nodes_status = fetch_status(etcd_client, 'nodes')
        if len(nodes_status) > EXPECTED_NODES:
            LOG.info("Looks like we have stale data in etcd, found %s nodes, "
                     "but expected to find %s, sleeping for %s sec",
                     len(nodes_status), EXPECTED_NODES, sleep)
            time.sleep(sleep)
        else:
            break
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
        return ("", True)
    else:
        LOG.info("Joining to nodes %s", ','.join(status))
        return (','.join(status), False)


def update_uuid(etcd_client):

    wsrep_data = fetch_wsrep_data()
    uuid = wsrep_data['wsrep_cluster_state_uuid']
    key = os.path.join(ETCD_PATH, 'uuid')
    retry(etcd_set, CONNECTION_ATTEMPTS)(etcd_client, key, uuid, ttl=None)


def update_cluster_state(etcd_client, state):

    key = os.path.join(ETCD_PATH, 'state')
    retry(etcd_set, CONNECTION_ATTEMPTS)(etcd_client, key, state, ttl=None)


def wait_for_mysqld(proc):

    code = proc.wait()
    LOG.info("Process exited with code %d", code)
    sys.exit(code)


def wait_for_mysqld_to_start(proc, insecure):

    LOG.info("Waiting mysql to start...")
    password = '' if insecure else MYSQL_ROOT_PASSWORD
    for i in range(0, 199):
        try:
            mysql_client = get_mysql_client(unix_socket=MYSQL_SOCK,
                                            password=password)
            mysql_exec(mysql_client, [("SELECT 1", None)])
            return
        except Exception:
            time.sleep(1)
    else:
        LOG.error("Mysql boot failed")
        raise RuntimeError("Process exited with code: %s" % proc.returncode)


def wait_for_mysqld_to_stop():

    """
    Since mysqld start wrapper first, we can't check for the executed proc
    exit code and be assured that mysqld itself is finished working. We have
    to check whole process group, so we're going to use pgrep for this.
    """

    LOG.info("Waiting for mysqld to finish working")
    for i in range(0, 29):
        proc = run_cmd("pgrep mysqld")
        proc.communicate()
        if proc.returncode == 0:
            time.sleep(1)
        else:
            LOG.info("Mysqld finished working")
            break
    else:
        LOG.info("Can't kill the mysqld process used for bootstraping")
        sys.exit(1)


def mysql_init():
    datadir_cleanup(DATADIR)
    run_cmd("mysqld --initialize-insecure", check_result=True)
    mysqld_proc = run_cmd("mysqld --skip-networking")
    wait_for_mysqld_to_start(mysqld_proc, insecure=True)

    LOG.info("Mysql is running, setting up the permissions")
    sql_list = [("CREATE USER 'root'@'%%' IDENTIFIED BY %s",
                MYSQL_ROOT_PASSWORD),
                ("GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION", None),
                ("ALTER USER 'root'@'localhost' IDENTIFIED BY %s",
                MYSQL_ROOT_PASSWORD),
                ("CREATE USER 'xtrabackup'@'localhost' IDENTIFIED BY %s",
                XTRABACKUP_PASSWORD),
                ("GRANT RELOAD,PROCESS,LOCK TABLES,REPLICATION CLIENT ON *.*"
                " TO 'xtrabackup'@'localhost'", None),
                ("GRANT REPLICATION CLIENT ON *.* TO monitor@'%%' IDENTIFIED"
                " BY %s", MONITOR_PASSWORD),
                ("DROP DATABASE IF EXISTS test", None),
                ("FLUSH PRIVILEGES", None)]
    try:
        mysql_client = retry(get_mysql_client, 3, 1)(unix_socket=MYSQL_SOCK)
        mysql_exec(mysql_client, sql_list)
    except Exception:
        raise

    create_init_flag()
    # It's more safe to kill mysqld via pkill, since mysqld start wrapper first
    run_cmd("pkill mysqld")
    wait_for_mysqld_to_stop()
    LOG.info("Mysql bootstraping is done")


def check_cluster(etcd_client):

    state = get_cluster_state(etcd_client)
    nodes_status = fetch_status(etcd_client, 'nodes')
    if not nodes_status and state == 'STEADY':
        LOG.warning("Cluster is in the STEADY state, but there no"
                    " alive nodes detected, running cluster recovery")
        update_cluster_state(etcd_client, 'RECOVERY')


def acquire_lock(lock, ttl):

    LOG.info("Locking...")
    lock.acquire(blocking=True, lock_ttl=ttl)
    LOG.info("Successfuly acquired lock")


def release_lock(lock):

    lock.release()
    LOG.info("Successfuly released lock")


def set_safe_to_bootstrap():

    """
    Less wordy way to do "inplace" edit of the file
    """

    for line in fileinput.input(GRASTATE_FILE, inplace=1):
        if line.startswith("safe_to_bootstrap"):
            line = line.replace("safe_to_bootstrap: 0", "safe_to_bootstrap: 1")
            sys.stdout.write(line)


def run_create_queue(etcd_client, lock, ttl):

    """
    In this step we're making recovery preparations.

    We need to get our seqno from mysql, after that we done, we'll fall into
    the endless loop waiting 'till other nodes do the same and after that we
    wait for our turn, based on the seqno, to start jointing the cluster.
    """

    LOG.info("Creating recovery queue")
    key = os.path.join(ETCD_PATH, 'queue', IPADDR)
    etcd_register_in_path(etcd_client, key, ttl=None)
    etcd_set_seqno(etcd_client, ttl=None)
    release_lock(lock)
    wait_for_expected_state(etcd_client, ttl)


def run_join_cluster(etcd_client, lock, ttl):

    """
    In this step we're ready to join or create new cluster.

    We get current nodes list, and it's empty it means we're the first one.
    If the seqno queue list is empty and nodes list is equals to 3, we assume
    that we're the last one. In the one last case we're the second one.

    If we're the first one, we're creating the new cluster.
    If we're the second one or last one, we're joinning to the existing
    cluster.

    If cluster state was a RECOVERY we do the same thing, but nodes take turns
    not by first come - first served rule, but by the seqno of their data, so
    first one node will the one with the most recent data.
    """

    LOG.info("Joining the cluster")
    acquire_lock(lock, ttl)
    state = get_cluster_state(etcd_client)
    nodes_status = fetch_status(etcd_client, 'nodes')
    available_nodes, first_one = create_join_list(nodes_status)
    if first_one:
        set_safe_to_bootstrap()
    mysqld = run_mysqld(available_nodes, etcd_client, lock)
    wait_for_sync(mysqld)
    key = os.path.join(ETCD_PATH, 'nodes', IPADDR)
    etcd_register_in_path(etcd_client, key, ttl)
    if state == "RECOVERY":
        for path in ['seqno', 'queue']:
            key = os.path.join(ETCD_PATH, path, IPADDR)
            etcd_deregister_in_path(etcd_client, key)
    last_one = check_if_im_last(etcd_client)
    release_lock(lock)
    return (first_one, last_one, mysqld)


def run_update_metadata(etcd_client, first_one, last_one):

    """
    In this step we updating the cluster state and metadata.

    If node was the first one, it change the state of the cluster to the
    BUILDING and sets it's uuid as a cluster uuid in etcd.

    If node was the last one it change the state of the cluster to the STEADY.

    Please note, that if it was a RECOVERY scenario, we dont change state of
    the cluster until it will be fully rebuilded.
    """

    LOG.info("Update cluster metadata")
    state = get_cluster_state(etcd_client)
    if first_one:
        update_uuid(etcd_client)
        if state != 'RECOVERY':
            update_cluster_state(etcd_client, 'BUILDING')
    if last_one:
        update_cluster_state(etcd_client, 'STEADY')


def main(ttl):

    if not os.path.isfile(INIT_FILE):
        LOG.info("Init file '%s' not found, doing full init", INIT_FILE)
        mysql_init()
    else:
        LOG.info("Init file '%s' found. Skiping mysql bootstrap and run"
                 " wsrep-recover", INIT_FILE)
        run_cmd("mysqld_safe --wsrep-recover", check_result=True)

    try:
        LOG.debug("My IP is: %s", IPADDR)
        etcd_client = retry(get_etcd_client, CONNECTION_ATTEMPTS)(ETCD_HOSTS)
        lock = etcd.Lock(etcd_client, 'galera')
        acquire_lock(lock, ttl)
        check_cluster(etcd_client)
        state = get_cluster_state(etcd_client)

        # Scenario 1: Initial bootstrap
        if state is None or state == 'BUILDING':
            LOG.info("No running cluster detected - starting bootstrap")
            first_one, last_one, mysqld = run_join_cluster(etcd_client, lock,
                                                           ttl)
            run_update_metadata(etcd_client, first_one, last_one)
            LOG.info("Bootsraping is done. Node is ready.")

        # Scenario 2: Re-connect
        elif state == 'STEADY':
            LOG.info("Detected running cluster, re-connecting")
            first_one, last_one, mysqld = run_join_cluster(etcd_client, lock,
                                                           ttl)
            LOG.info("Node joined and ready")

        # Scenario 3: Recovery
        elif state == 'RECOVERY':
            LOG.warning("Cluster is in the RECOVERY state, re-connecting to"
                        " the node with the oldest data")
            run_create_queue(etcd_client, lock, ttl)
            first_one, last_one, mysqld = run_join_cluster(etcd_client, lock,
                                                           ttl)
            run_update_metadata(etcd_client, first_one, last_one)
            LOG.info("Recovery is done. Node is ready.")

        wait_for_mysqld(mysqld)
    except Exception:
        raise
    finally:
        for path in ['seqno', 'queue', 'nodes', 'leader']:
            key = os.path.join(ETCD_PATH, path, IPADDR)
            prevValue = IPADDR if path == 'leader' else False
            etcd_deregister_in_path(etcd_client, key, prevValue=prevValue)
        release_lock(lock)


if __name__ == "__main__":
    config = get_config(GLOBALS_PATH, ['percona', 'db', 'etcd', 'namespace'])
    set_globals(config)
    main(ttl=300)

# vim: set ts=4 sw=4 tw=0 et :
