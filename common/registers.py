# coding=utf-8
import socket
import ujson as json

import redis

from common.connections import RedisConnectionFactory, ZkClientFactory
from common.loggers import app_log

from common.utils import get_host_name, get_client_id, get_pid
from search_platform import settings

__author__ = 'liuzhaoming'


class RegisterCenter(object):
    """
    注册中心，支持应用注册到注册中心，心跳，获取应用信息
    """
    def register(self):
        """
        注册应用
        """
        pass
    def get_hosts(self):
        """
        获取所有注册的主机地址
        """
        pass
class RedisRegisterCenter(RegisterCenter):
    """
    注册中心，支持应用注册到注册中心，心跳，获取应用信息
    """

    def register(self):
        """
        注册应用，如果应用已经注册，那么修改注册时间
        :return:
        """
        app_log.info('Register center register is called')
        conn_pool = None
        try:
            redis_host = settings.SERVICE_BASE_CONFIG['redis']
            redis_conn = RedisConnectionFactory.get_redis_connection(redis_host)
            # conn_pool = redis.ConnectionPool.from_url(redis_host)
            # redis_conn = redis.Redis(connection_pool=conn_pool)
            redis_conn.sadd(settings.SERVICE_BASE_CONFIG['register_center_key'], self.__get_register_info())
        except Exception as e:
            app_log.error('Register center register has error ', e)
        finally:
            if conn_pool:
                conn_pool.disconnect()

    def get_hosts(self):
        """
        获取所有注册的主机地址
        :return:
        """
        conn_pool = None
        try:
            redis_host = settings.SERVICE_BASE_CONFIG['redis']
            conn_pool = redis.ConnectionPool.from_url(redis_host)
            redis_conn = redis.Redis(connection_pool=conn_pool)
            return list(redis_conn.smembers(settings.SERVICE_BASE_CONFIG['register_center_key']))
        except Exception as e:
            app_log.error('Register center get hosts has error ', e)
            return []
        finally:
            if conn_pool:
                conn_pool.disconnect()

    def __get_register_info(self):
        """
        获取注册信息
        :return:
        """
        host_name = get_host_name()
        host_address = socket.gethostbyname(host_name)
        return host_address


class ZookeeperRegisterCenter(RegisterCenter):
    """
    zk 注册中心
    """
    def __init__(self):
        self.zk = ZkClientFactory.get_zk_client()
        self.register_path = '/app/search_platform/register'
    def register(self):
        """
        注册应用
        :return:
        """
        try:
            self.zk.ensure_path(self.register_path)
            cnode_key, cnode_value = self.__get_register_info()
            self.zk.create('{0}/{1}'.format(self.register_path, cnode_key), ephemeral=True, value=cnode_value)
        except Exception as e:
            app_log.error('register to zk fail {0}', e, get_client_id())
    def get_hosts(self):
        """
        获取所有注册的主机地址
        :return:
        """
        cnode_key_list = self.zk.get_children(self.register_path)
        return list(set(map(lambda cnode_key: cnode_key.split('_')[0], cnode_key_list)))
    def __get_register_info(self):
        """
        获取注册信息
        :return:
        """
        host_name = get_host_name()
        host_address = socket.gethostbyname(host_name)
        pid = get_pid()
        register_info = {'host_name': host_name, 'host_address': host_address, 'pid': pid}
        return '{0}_{1}'.format(host_address, pid), json.dumps(register_info)
register_center = ZookeeperRegisterCenter()
if __name__ == '__main__':
    register_center.register()
    print register_center.get_hosts()
