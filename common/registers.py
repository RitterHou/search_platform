# coding=utf-8
import socket

import redis

from common.loggers import app_log

from common.utils import get_host_name
from search_platform import settings

__author__ = 'liuzhaoming'


class RegisterCenter(object):
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
            conn_pool = redis.ConnectionPool.from_url(redis_host)
            redis_conn = redis.Redis(connection_pool=conn_pool)
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
        host_addr = socket.gethostbyname(host_name)
        return host_addr


register_center = RegisterCenter()
