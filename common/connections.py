# -*- coding: utf-8 -*-
import redis
from elasticsearch import Elasticsearch, Transport, ElasticsearchException, TransportError
from dubbo_client import ZookeeperRegistry, DubboClient, ApplicationConfig

from common.utils import COMBINE_SIGN
from common.loggers import app_log
from search_platform import settings

__author__ = 'liuzhaoming'

BATCH_REQUEST_TIMEOUT = 30
BATCH_TIMEOUT = 120000
INDEX_REQUEST_TIMEOUT = 120
INDEX_TIMEOUT = 120000
class EsConnection(Elasticsearch):
    """
    封装的ES操作接口，添加了多个索引
    """

    def __init__(self, hosts=None, transport_class=Transport, **kwargs):
        Elasticsearch.__init__(self, hosts, transport_class, **kwargs)
        self.host_list = hosts


class EsConnectionPool(object):
    """
    ES连接管理池，用装饰器实现单例
    """

    # ES连接字典，对应：{‘192.168.0.1：9200’：Esconnection,‘172.168.0.1：9200’：EscConnection}
    connection_cache = {}

    # ES索引是否初始化，只初始化一次,key为'index|||type'
    es_type_init_info_cache = {}

    def __init__(self):
        pass

    def get_es_connection(self, host=None, es_config=None, create_index=True):
        """
        获取ES连接
        :param host:
        :param es_config:
        :return:
        """
        from common.configs import config
        _host = host or [es_config['host'] if es_config and 'host' in es_config else None][0]
        _host = _host.format(**config.get_value('consts/custom_variables'))
        conn = self.__create_connection(_host)

        try:
            if create_index and es_config and 'index' in es_config and 'type' in es_config and 'mapping' in es_config \
                    and es_config['mapping']:
                type_key = ''.join((_host + COMBINE_SIGN, es_config['index'], COMBINE_SIGN, es_config['type']))
                if type_key in self.es_type_init_info_cache[_host]:
                    return conn
                app_log.info('Cache doesnot have type key {0}', type_key)
                if not conn.indices.exists(es_config['index']):
                    app_log.info('Creates index {0}', es_config['index'])
                    conn.indices.create(es_config['index'], body={"number_of_shards": "2"},
                                        params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                'timeout': INDEX_TIMEOUT})
                is_type_exist = conn.indices.exists_type(es_config['index'], es_config['type'])
                if not is_type_exist:
                    app_log.info('Creates type {0}', es_config['type'])
                    conn.indices.put_mapping(doc_type=es_config['type'], body=es_config['mapping'],
                                             index=es_config['index'], params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                                               'timeout': INDEX_TIMEOUT})
                self.es_type_init_info_cache[_host][type_key] = ''
        except ElasticsearchException as e:
            app_log.exception(e)

        return conn

    def __create_connection(self, host):
        """
        创建连接器，如果cache中存在该host的连接，则不再创建
        :param host:
        :return:
        """
        if host not in self.connection_cache:
            try:
                connection = EsConnection(host.split(','), sniff_on_start=True)
            except TransportError as e:
                app_log.error('create elasitcsearch connection fail, host={0}', e, host)
                if 'Unable to sniff hosts' in str(e):
                    connection = EsConnection(host.split(','), sniff_on_start=False)
            self.connection_cache[host] = connection
            self.es_type_init_info_cache[host] = {}
        else:
            connection = self.connection_cache[host]
        return connection


EsConnectionFactory = EsConnectionPool()


class DubboRegistryPool(object):
    """
    Dubbo相关接口缓存
    """

    def __init__(self):
        self.config = ApplicationConfig('search_platform')
        # DubboRegistry连接字典，对应：{‘192.168.0.1：9200’：DubboRegistry,‘172.168.0.1：9200’：DubboRegistry}
        self.connection_cache = {}
        self.dubbo_client_cache = {}

    def get_dubbo_client(self, host, service_interface, fields, version):
        if not host or not service_interface:
            app_log.error('Get bubbo client param is invalid, {0} {1}', host, service_interface)
            return None
        if host not in self.connection_cache:
            registry = ZookeeperRegistry(host, self.config)
            self.connection_cache[host] = registry
        dubbo_client_key = COMBINE_SIGN.join((host, service_interface))
        if dubbo_client_key not in self.dubbo_client_cache:
            dubbo_client = DubboClient(service_interface, self.connection_cache[host], version=version)
            self.dubbo_client_cache[dubbo_client_key] = dubbo_client
        return self.dubbo_client_cache[dubbo_client_key]


DubboRegistryFactory = DubboRegistryPool()

class RedisPool(object):
    def __init__(self):
        self.redis_conn_pool = {}
    def get_redis_connection(self, host):
        if not host:
            app_log.error('Get redis connection param is invalid, {0}', host)
            return None
        if host not in self.redis_conn_pool or not self.redis_conn_pool[host]:
            self.redis_conn_pool[host] = redis.ConnectionPool.from_url(host)
        return redis.Redis(connection_pool=self.redis_conn_pool[host])
RedisConnectionFactory = RedisPool()
if __name__ == '__main__':
    es_config = {'host': 'http://172.19.65.66:9200,http://172.19.65.79:9200', 'index': 'test-aaa', 'type': 'test-type',
                 'mapping': {"properties": {"category": {"index": "not_analyzed", "type": "string"}}}}
    EsConnectionFactory.get_es_connection(es_config=es_config, create_index=True)