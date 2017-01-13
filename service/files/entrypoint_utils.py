import functools
import json
import logging
import time

import etcd
import pymysql.cursors

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def retry(f, connection_attempts=3, connection_delay=1):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = connection_attempts
        delay = connection_delay
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


def get_config(globals_path, keys_list):

    LOG.info("Getting global variables from %s", globals_path)
    variables = {}
    with open(globals_path) as f:
        global_conf = json.load(f)
    for key in keys_list:
        variables[key] = global_conf[key]
    LOG.debug(variables)
    return variables


def get_etcd_client(etcd_machines):

    """
    Initialize the etcd client.

    Args:
        etcd_machines (tuple of tuples): ((host, port), (host, port), ...)
    """

    etcd_machines_str = " ".join(["%s:%d" % (h, p) for h, p in etcd_machines])
    LOG.debug("Using the following etcd urls: %s", etcd_machines_str)
    return etcd.Client(host=etcd_machines, allow_reconnect=True,
                       read_timeout=2, protocol='http')


def get_mysql_client(host='127.0.0.1', unix_socket='', port=33306, user='root',
                     password=''):
        mysql_client = pymysql.connect(host=host,
                                       unix_socket=unix_socket,
                                       port=port,
                                       user=user,
                                       password=password,
                                       connect_timeout=1,
                                       read_timeout=1,
                                       cursorclass=pymysql.cursors.DictCursor)
        return mysql_client


def etcd_set(etcd_client, key, value, ttl, dir=False, append=False, **kwargs):

    etcd_client.write(key, value, ttl, dir, append, **kwargs)
    LOG.info("Set %s with value '%s'", key, value)


def etcd_refresh(etcd_client, key, ttl):

    etcd_client.refresh(key, ttl)
    LOG.info("Refreshed %s ttl. New ttl is '%s'", key, ttl)


def etcd_delete(etcd_client, key, recursive=True, dir=True, **kwargs):

    etcd_client.delete(key, recursive, dir, **kwargs)
    LOG.warning("Deleted node's key '%s'", key)


def etcd_read(etcd_client, key):

    return etcd_client.read(key).value


def etcd_get(etcd_client, key):

    return etcd_client.get(key)


def etcd_register_in_path(etcd_client, key, ttl):

    etcd_set(etcd_client, key, time.time(), ttl)


def etcd_deregister_in_path(etcd_client, key, prevValue=False):

    try:
        if prevValue:
            etcd_delete(etcd_client, key, recursive=False, dir=False,
                        prevValue=prevValue)
        else:
            etcd_delete(etcd_client, key, recursive=True)
        LOG.warning("Deleted key %s", key)
    except etcd.EtcdKeyNotFound:
        LOG.warning("Key %s not exist", key)


# vim: set ts=4 sw=4 tw=0 et :
