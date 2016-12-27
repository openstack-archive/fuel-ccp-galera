import functools
import logging
import socket
import subprocess
import six

HOSTNAME = socket.getfqdn()
IPADDR = socket.gethostbyname(HOSTNAME)
BACKEND_NAME = "galera-cluster"
SERVER_NAME = "primary"

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
    for key in ['etcd', 'namespace']:
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
    ETCD_HOST = "etcd.%s" % config['namespace']
    ETCD_PORT = int(config['etcd']['client_port']['cont'])


def get_etcd_client():

    return etcd.Client(host=ETCD_HOST,
                       port=ETCD_PORT,
                       allow_reconnect=True,
                       read_timeout=2)


def get_socket():
    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_socket.settimeout(0.1)
    unix_socket.connect('/var/run/haproxy/admin.sock')
    return unix_socket

def run_haproxy():

    cmd = ["haproxy", "-f", "/etc/haproxy/haproxy.conf" ]
    LOG.info("Executing cmd:\n%s", cmd)
    proc = subprocess.Popen(cmd)
    return proc


@retry
def _etcd_set(etcd_client, path, value, ttl):

    key = os.path.join(ETCD_PATH, path)
    etcd_client.set(key, value, ttl=ttl)
    LOG.info("Set %s with value '%s'", key, value)


def send_command(cmd):
    sock = get_socket()
    sock.send(six.b(cmd + '\n'))
    file_handle = sock.makefile()
    data = file_handle.read().splitlines()
    return data

def get_server_status():
    cmd = "show servers state galera-cluster"
    data = send_command(cmd)
    keys = data[1].split(' ')
    keys.pop(0)
    values = data[2].split(' ')
    data_dict = dict(zip(keys, values))
    return data_dict

def set_server_addr(leader_ip):
    cmd = "set server %s/%s addr %s port 33306" %
          (BACKEND_NAME, SERVER_NAME, leader_ip)
    send_command(cmd)

#get_server_status()
#set_server_addr()

if __name__ == "__main__":
    get_config()
    set_globals()
    main(ttl=30)

# vim: set ts=4 sw=4 tw=0 et :
