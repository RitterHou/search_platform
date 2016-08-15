# coding=utf-8
import copy
from itertools import chain, groupby
import xmlrpclib
from collections import OrderedDict
import ujson as json
import urllib2

from re import search
import redis

from common.admin_config import admin_config
from common.connections import EsConnectionFactory, RedisConnectionFactory
from common.es_routers import es_router
from common.msg_bus import message_bus, Event
from common.configs import config, config_holder
from common.exceptions import UpdateDataNotExistError, InvalidParamError
from common.loggers import query_log as app_log
from common.adapter import es_adapter
from common.registers import register_center
from common.utils import get_dict_value_by_path, bind_dict_variable, merge, unbind_variable
from river import get_river_key
from river.rivers import process_syn_message
from search_platform import settings
from search_platform.settings import SERVICE_BASE_CONFIG
from service.models import SearchPlatformDoc, Product
from suggest.data_processings import data_processing
from suggest.destinations import suggest_destination
from suggest.notifications import SuggestNotification
from suggest.sources import suggest_source


__author__ = 'liuzhaoming'


class DataRiver(object):
    """
    数据流
    """

    def get(self, river_name=None):
        """
        获取数据流，如果指定了名称，则返回给定名称的数据流
        :param river_name:
        :return:
        """
        # data_river_cfg = es_adapter.get_config('data_river')
        # cfg_rivers = data_river_cfg['data_river'].get('rivers')
        cfg_rivers = config.get_value('data_river/rivers')
        if not cfg_rivers:
            return []
        if river_name:
            return filter(lambda river: river.get('name') == river_name, cfg_rivers)
        return cfg_rivers

    def save(self, _data_river):
        """
        新增加数据流
        :param _data_river:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        cfg_rivers.append(_data_river)
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})

    def update(self, _data_river):
        """
        更新数据流，如果同名的数据流不存在，则返回错误
        :param _data_river:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        filter_river_list = filter(lambda (index, river): river.get('name') == _data_river.get('name'),
                                   list(cfg_rivers))
        if 0 == len(filter_river_list):
            raise UpdateDataNotExistError()
        cfg_rivers[filter_river_list[0][0]] = _data_river
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})

    def delete(self, name):
        """
        删除数据流
        :param name:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        filter_river_list = filter(lambda (index, river): river.get('name') == name, enumerate(cfg_rivers))
        if not len(filter_river_list):
            app_log.info('delete data river not exist {0}', name)
            return

        del cfg_rivers[filter_river_list[0][0]]
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})


class EsTmpl(object):
    """
    ES模板
    """

    def get(self, tmpl_name=None):
        """
        获取ES模板，如果指定了名称，则返回给定名称的ES模板
        :param tmpl_name:
        :return:
        """
        # tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        tmpl_cfg = config.get_value('es_index_setting')
        if not tmpl_cfg:
            app_log.info("Get EsTmpl cannot find es_index_setting")
            return []
        tmpl_list = map(lambda (key, value): OrderedDict(value, **{'name': key}), tmpl_cfg.iteritems())
        if tmpl_name:
            return filter(lambda item: item.get('name') == tmpl_name, tmpl_list)
        return tmpl_list

    def save(self, tmpl):
        """
        新增加ES模板
        :param tmpl:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        tmpl_cfg[tmpl.get('name')] = tmpl
        del tmpl['name']
        es_adapter.save_config({'es_index_setting': tmpl_cfg})

    def update(self, tmpl):
        """
        更新ES模板，如果同名的ES模板不存在，则返回错误
        :param tmpl:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        tmpl_cfg[tmpl.get('name')] = tmpl
        del tmpl['name']
        es_adapter.save_config({'es_index_setting': tmpl_cfg})

    def delete(self, name):
        """
        删除ES模板
        :param name:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        del tmpl_cfg[name]
        es_adapter.save_config({'es_index_setting': tmpl_cfg})


class QueryChain(object):
    """
    HTTP处理链
    """

    def get(self, handler_name=None):
        """
        获取REST处理器，如果指定了名称，则返回给定名称的REST处理器
        :param handler_name:
        :return:
        """
        # query_chain_cfg = es_adapter.get_config('query')
        # cfg_handlers = query_chain_cfg['query'].get('chain')
        cfg_handlers = config.get_value('/query/chain')
        if not cfg_handlers:
            app_log.info("Get QueryChain cannot find query")
            return []
        if handler_name:
            return filter(lambda handler: handler.get('name') == handler_name, cfg_handlers)
        return cfg_handlers

    def save(self, handler):
        """
        新增加REST处理器
        :param handler:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        cfg_handlers.append(handler)
        es_adapter.save_config({'query': {'chain': cfg_handlers}})

    def update(self, handler):
        """
        更新REST处理器，如果同名的REST处理器不存在，则返回错误
        :param handler:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        filter_handler_list = filter(lambda (index, river): river.get('name') == handler.get('name'),
                                     enumerate(cfg_handlers))
        if not len(filter_handler_list):
            raise UpdateDataNotExistError()
        cfg_handlers[filter_handler_list[0][0]] = handler
        es_adapter.save_config({'query': {'chain': cfg_handlers}})

    def delete(self, name):
        """
        删除REST处理器
        :param name:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        filter_handler_list = filter(lambda (index, river): river.get('name') == name,
                                     enumerate(cfg_handlers))
        if not len(filter_handler_list):
            app_log.info('delete query chain not exist {0}', name)
            return

        del cfg_handlers[filter_handler_list[0][0]]
        es_adapter.save_config({'query': {'chain': cfg_handlers}})


class SystemParam(object):
    """
    系统配置参数
    """

    def get(self):
        """
        获取系统参数
        :return:
        """
        # sys_param_cfg = es_adapter.get_config('consts').get('consts')
        sys_param_cfg = config.get_value('consts')
        if not sys_param_cfg:
            app_log.info("Get System Param cannot find consts")
            return {}
        return sys_param_cfg

    def save(self, sys_params):
        """
        更新系统参数
        :param sys_params:
        :return:
        """
        if not sys_params:
            app_log.info("Get System Param input sys param is None")
            return
        # system_param_cfg = es_adapter.get_config('consts')
        # system_param_cfg['consts'] = sys_params
        es_adapter.save_config({'consts': sys_params})


class Supervisor(object):
    def get_cluster_supervisor_info(self, host_addr=None):
        """
        获取搜索平台supervisor信息
        :param host_addr:
        :return:
        """
        hosts = self.__get_hosts(host_addr)
        return filter(lambda item: item, map(self.get_supervisor_info, hosts))

    def get_supervisor_info(self, host_info):
        """
        获取单个服务器上supervisor相关信息
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        try:
            supervisor_info = self.get_supervisor_process_info(host_info)
            supervisor_info['sub_process_list'] = self.get_supervisor_sub_process_list(host_info)
            return supervisor_info
        except Exception as e:
            app_log.error('get_supervisor_info has error {0}', e, host_info)

    def get_supervisor_process_info(self, host_info):
        """
        获取supervisor进程信息
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        supervisor_proxy = self.__get_proxy(host_info=host_info)
        state = supervisor_proxy.getState()
        pid = supervisor_proxy.getPID()
        return {'state': state['statename'], 'pid': pid, 'host': host_info['host']}

    def get_supervisor_sub_process_list(self, host_info):
        """
        获取supervisor子进程集合
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        supervisor_proxy = self.__get_proxy(host_info=host_info)
        process_info_list = supervisor_proxy.getAllProcessInfo()
        return process_info_list

    def do_action(self, host, action, process_name=None):
        app_log.info('do_action is called host={0} , action={1}', host, action)
        host_info_list = self.__get_hosts(host)
        if not host_info_list:
            app_log.info('cannot find proxy {0}', host)
            return None
        if action == 'start':
            return self.__do_process_start(host_info_list, process_name)
        elif action == 'stop':
            return self.__do_process_stop(host_info_list, process_name)
        elif action == 'restart':
            return self.__do_process_restart(host_info_list, process_name)
        elif action == 'clear_log':
            return self.__do_process_log_clear(host_info_list, process_name)
        elif action == 'get_log':
            return self.__get_process_log(host_info_list, process_name)


    def __do_process_start(self, host_list, process_name=None):
        """
        启动进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.startProcessGroup(process_name)
                else:
                    supervisor_proxy.startAllProcesses()
            except Exception as e:
                app_log.exception(e)
                raise e

    def __do_process_stop(self, host_list, process_name=None):
        """
        停止进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.stopProcessGroup(process_name)
                else:
                    supervisor_proxy.stopAllProcesses()
            except Exception as e:
                app_log.exception(e)
                raise e

    def __do_process_restart(self, host_list, process_name=None):
        """
        重启进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.stopProcessGroup(process_name)
                    supervisor_proxy.startProcessGroup(process_name)
                else:
                    supervisor_proxy.stopAllProcesses()
                    supervisor_proxy.startAllProcesses()
            except Exception as e:
                app_log.exception(e)
                raise e

    def __do_process_log_clear(self, host_list, process_name=None):
        """
        清除进程日志
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.clearProcessLogs(process_name)
                else:
                    supervisor_proxy.clearAllProcessLogs()
            except Exception as e:
                app_log.exception(e)
                raise e

    def __get_process_log(self, host_list, process_name=None):
        """
        清除进程日志
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    return supervisor_proxy.readProcessLog(process_name, 0, 0)
                else:
                    return supervisor_proxy.readMainLog(0, 0)
            except Exception as e:
                app_log.exception(e)
                raise e

    def __get_hosts(self, host_addr=None):
        """
        根据给定的host地址获取host信息，如果制定的host地址为空，则返回所有的host信息
        :param host_addr:
        :return:
        """
        hosts = self.__get_all_hosts()
        if host_addr:
            hosts = filter(lambda host: host['host'] == host_addr, hosts)
        return hosts


    def __get_proxy(self, host_url=None, host_info=None):
        """
        获取XML-RPC代理
        :param host_url:
        :param host_info:
        :return:
        """
        if host_url:
            __host_url = host_url
        elif host_info:
            __host_url = 'http://' + host_info['host'] + ':' + host_info['supervisor_port'] + '/RPC2'
        if __host_url not in SUPERVISOR_PROXY_CACHE:
            server = xmlrpclib.ServerProxy(__host_url)
            proxy = server.supervisor
            SUPERVISOR_PROXY_CACHE[__host_url] = proxy
        return SUPERVISOR_PROXY_CACHE[__host_url]

    def __get_all_hosts(self):
        """
        获取搜索引擎所有的主机信息
        :return:
        """
        host_list = config.get_value('consts/manager/hosts')
        default_host_cfg = config.get_value('consts/manager/default')
        register_hosts = map(lambda host: dict(default_host_cfg, host=host), register_center.get_hosts())
        all_hosts = host_list + register_hosts

        return map(lambda (m, n): list(n)[0], groupby(all_hosts, lambda host_info: host_info['host'].strip()))


class Message(object):
    """
    系统消息管理
    """

    def get(self):
        """
        获取发送的消息列表
        :return:
        """
        host = SERVICE_BASE_CONFIG['elasticsearch']
        msg_store = config.get_value('consts/manager/message/es_store')
        msg_query_result = es_adapter.search(query_body=None, host=host, index=msg_store['index'],
                                             doc_type=msg_store['type'])
        return msg_query_result['root']

    def send(self, msg):
        """
        发送并保存消息
        :param msg:
        :return:
        """
        host = SERVICE_BASE_CONFIG['elasticsearch']
        msg_store = config.get_value('consts/manager/message/es_store')
        es_adapter.create_doc(host=host, index=msg_store['index'], doc_type=msg_store['type'], doc=msg)

        message_bus.publish(**msg)


class AnsjSegmentation(object):
    """
    ANSJ分词管理
    """

    def __init__(self):
        self.init = False

    def set_segmentation(self, segmentation):
        """
        设置ANSJ分词词汇
        :param segmentation:
        :return:
        """
        if not self.init:
            self.init_config()
            self.init = True
        msg = self.__to_msg(segmentation)
        if msg:
            self.redis_conn.publish(self.redis_cfg['channel'], msg)

    def init_config(self):
        self.redis_cfg = config.get_value('consts/global/ansj_segment_redis')
        pool = redis.ConnectionPool.from_url(self.redis_cfg['host'])
        self.redis_conn = redis.Redis(connection_pool=pool)


    def __to_msg(self, segmentation):
        """
        将分词数据转化为Redis命令
        :param segmentation:
        :return:
        """
        str_buffer = []
        if segmentation['type'] == 'user_define':
            str_buffer.append('u')
            if segmentation['operator'] == 'add':
                str_buffer.append('c')
            elif segmentation['operator'] == 'delete':
                str_buffer.append('d')
        elif segmentation['type'] == 'ambiguity':
            str_buffer.append('a')
            if segmentation['operator'] == 'add':
                str_buffer.append('c')
            elif segmentation['operator'] == 'delete':
                str_buffer.append('d')
        str_buffer.append(segmentation['text'])
        return ':'.join(str_buffer)


class Suggest(object):
    """
    Suggest拼写建议
    """

    def query_suggest_terms(self, admin_id, params):
        """
        获取用户ID下所有的拼写提示词
        :param admin_id:
        :param params
        :return:
        """
        if not admin_id:
            raise InvalidParamError('The adminId cannot be null')

        yun_product_suggest_cfg = self._get_admin_suggest_river(admin_id)

        if not yun_product_suggest_cfg:
            app_log.error('Cannot find product suggest config, adminId={0}', admin_id)
            return
        destination_config = get_dict_value_by_path('destination', yun_product_suggest_cfg)[0]
        variable_values = {'version': config.get_value('version'), 'adminId': admin_id}
        es_config = es_router.merge_es_config(destination_config)
        es_config = es_router.route(es_config, variable_values)
        index, es_type, doc_id = es_adapter.get_es_doc_keys(es_config, kwargs=variable_values)

        size = params.get('size') or 100
        if size > 200 and params.get('whoami') != 'god':
            size = 200
        from_size = params.get('from') or 0
        query_result = es_adapter.search(query_body={'from': from_size, 'size': size}, index=index,
                                         host=es_config['host'], doc_type=es_type)

        return {'root': map(lambda es_suggest_doc: self.to_suggestion(es_suggest_doc, ''), query_result['root']),
                'total': query_result['total']}

    def add_suggest_term(self, suggestion):
        """
        增加提示词
        多个提示词写在word中
        :param suggestion:
        :return:
        """
        yun_product_suggest_cfg = self._get_admin_suggest_river(suggestion['adminId'])

        if not yun_product_suggest_cfg:
            app_log.error('Cannot find admin suggest config {0}', suggestion)
            return

        yun_product_suggest_cfg['source']['type'] = 'specify_words'
        data_processing_config = get_dict_value_by_path('processing', yun_product_suggest_cfg)
        source_docs = suggest_source.pull(yun_product_suggest_cfg, suggestion)
        processed_data = data_processing.process_data(data_processing_config, source_docs, yun_product_suggest_cfg)
        suggest_destination.push(yun_product_suggest_cfg, processed_data)

    def delete_suggest_term(self, suggestion):
        """
        删除提示词
        :param suggestion:
        :return:
        """
        yun_product_suggest_cfg = self._get_admin_suggest_river(suggestion['adminId'])

        if not yun_product_suggest_cfg:
            app_log.error('Cannot find admin suggest config {0}', suggestion)
            return

        yun_product_suggest_cfg['source']['type'] = 'specify_words'
        for destination in yun_product_suggest_cfg['destination']:
            # destination['destination_type'] = 'elasticsearch'
            destination['operation'] = 'delete'
        # suggestion['host'], suggestion['index'], suggestion['type'] = self.__get_product_es_setting(suggestion)
        data_processing_config = get_dict_value_by_path('processing', yun_product_suggest_cfg)
        source_docs = suggest_source.pull(yun_product_suggest_cfg, suggestion)
        processed_data = data_processing.process_data(data_processing_config, source_docs, yun_product_suggest_cfg)
        suggest_destination.push(yun_product_suggest_cfg, processed_data)

    def init_suggest_index(self, admin_id):
        """
        手工执行suggest的全表扫描分词
        :param admin_id:
        :return:
        """
        is_vip = admin_config.is_vip(admin_id)
        yun_product_suggest_cfg = self._get_admin_suggest_river(admin_id)
        if not yun_product_suggest_cfg:
            app_log.error('Cannot find init suggest product suggest config')
            return
        notification_config = get_dict_value_by_path('notification', yun_product_suggest_cfg)

        # 手工执行的时候设置为先清除掉自动分词生成的提示词
        SuggestNotification().notify(notification_config, yun_product_suggest_cfg,
                                     {'adminId': admin_id, 'isVip': is_vip})

    def __get_product_es_setting(self, suggestion):
        """
        获取拼写建议对应的商品ElasticSearch 的index和type
        :param suggestion:
        :return:
        """
        if suggestion['adminId'] != 'gonghuo':
            # 云销商品
            es_setting_cfg = config.get_value('es_index_setting/product')
        else:
            # 供货商品
            es_setting_cfg = config.get_value('es_index_setting/gonghuo_product')

        if 'version' not in suggestion:
            suggestion['version'] = config.get_value('version')
        es_setting_cfg = bind_dict_variable(es_setting_cfg, suggestion)
        return es_setting_cfg['host'], es_setting_cfg['index'], es_setting_cfg['type']

    def to_suggestion(self, es_suggest_doc, product_type):
        """
        将ES查询的Suggest文档转换为标准格式的
        :param es_suggest_doc:
        :param product_type:
        :return:
        """
        suggestion = {'word': es_suggest_doc['name'],
                      'source_type': u'自动分词' if es_suggest_doc['suggest']['payload']['source_type'] == '1' else u'手工添加',
                      'hits': es_suggest_doc['suggest']['payload']['hits']}
        return suggestion
    def _get_admin_suggest_river(self, admin_id):
        """
        获取用户的suggest river配置
        """
        is_vip = admin_config.is_vip(admin_id)
        suggest_rivers = config.get_value('suggest/rivers')
        filter_list = filter(lambda river: river.get('name') == (
            'vip_admin_suggest_task' if is_vip else 'experience_admin_suggest_task'), suggest_rivers)
        yun_product_suggest_cfg = copy.deepcopy(filter_list[0]) if len(filter_list) else None
        return yun_product_suggest_cfg


class EsIndex(object):
    """
    ES索引操作接口
    """

    def add_index(self, _es_index):
        """
        增加索引
        """
        if not _es_index:
            return
        if 'host' not in _es_index:
            _es_index['host'] = self._get_default_es_host()

        es_connection = EsConnectionFactory.get_es_connection(host=_es_index['host'])
        if not es_connection.indices.exists(_es_index['index']):
            app_log.info('Creates index {0}', _es_index['index'])
            es_connection.indices.create(_es_index['index'], body={"number_of_shards": "2"})
        is_type_exist = es_connection.indices.exists_type(_es_index['index'], _es_index['type'])
        if not is_type_exist:
            app_log.info('Creates type {0}', _es_index['type'])
            es_connection.indices.put_mapping(doc_type=_es_index['type'], body=_es_index['mapping'],
                                              index=_es_index['index'])

    def delete_type(self, es_index):
        """
        删除索引下的type
        """
        if not es_index:
            return
        if 'host' not in es_index:
            es_index['host'] = self._get_default_es_host()

        es_connection = EsConnectionFactory.get_es_connection(host=es_index['host'])
        if es_connection.indices.exists_type(es_index['index'], es_index['type']):
            app_log.info('Delete type {0} {1}', es_index['index'], es_index['type'])
            es_connection.indices.delete_mapping(es_index['index'], es_index['type'])

    def delete_index(self, es_index):
        if not es_index:
            return
        if 'host' not in es_index:
            es_index['host'] = self._get_default_es_host()

        es_connection = EsConnectionFactory.get_es_connection(host=es_index['host'])
        if es_connection.indices.exists_type(es_index['index'], es_index['type']):
            app_log.info('Delete index {0}', es_index['index'])
            es_connection.indices.delete(es_index['index'])


    def delete_all_index_docs(self, _es_index):
        """
        删除索引中的所有文档
        """
        if not _es_index:
            return
        if 'host' not in _es_index:
            _es_index['host'] = self._get_default_es_host()

        es_connection = EsConnectionFactory.get_es_connection(host=_es_index['host'])
        app_log.info('Delete all products in {0} {1}', _es_index['index'], _es_index['type'])
        es_connection.delete_by_query(index=_es_index['index'], doc_type=_es_index['type'],
                                      body={"query": {"match_all": {}}})

    def query_es_index_info_list(self, shop_es_dict, match_type='equal'):
        """
        查询ES index 信息
        :param shop_es_dict:
        :return:
        """
        if 'host' not in shop_es_dict:
            shop_es_dict['host'] = self._get_default_es_host()

        es_connection = EsConnectionFactory.get_es_connection(host=shop_es_dict['host'])
        # mapping_dict = es_connection.indices.get_mapping()
        # index_type_dict_list = list(chain(
        # *[self._parse_index_mapping(index_name, mapping_dict[index_name]) for index_name in mapping_dict]))
        # matched_index_type_dict_list = filter(
        # lambda index_type_dict: self._filter_es_index(index_type_dict['index'], index_type_dict['type'],
        # shop_es_dict), index_type_dict_list)
        # self._merge_actual_doc_num(matched_index_type_dict_list, es_connection)
        matched_index_type_dict_list = self._merge_index_stats_info(shop_es_dict, es_connection, match_type)
        return matched_index_type_dict_list

    def _parse_index_mapping(self, index_name, mapping):
        """
        解析索引mapping文件，获取(index, type)列表
        :param index_name:
        :param mapping:
        :return:
        """
        if not mapping or not index_name:
            return ()

        __mappings = mapping.get('mappings')
        if not __mappings:
            return ()
        return [{'index': index_name, 'type': type_name} for type_name in mapping.get('mappings')]

    def _filter_es_index(self, index_name, doc_type, es_cfg):
        """
        判断ElasticSearch的索引和type是否符合条件
        :param index_name:
        :param doc_type:
        :param es_cfg:
        :return:
        """
        if es_cfg.get('index'):
            index_filter_result = True if search(es_cfg['index'], index_name) else False
        else:
            index_filter_result = True
        if es_cfg.get('type'):
            doc_type_filter_result = True if search(es_cfg['type'], doc_type) else False
        else:
            doc_type_filter_result = True
        return index_filter_result and doc_type_filter_result

    def _match_field(self, value, expr, match_type):
        """
        值匹配
        :param value:
        :param expr:
        :param match_type:
        :return:
        """
        if match_type == 'regex':
            return True if search(expr, value) else False
        else:
            return value == expr

    def _merge_index_stats_info(self, es_dict, es_connection, match_type='equal'):
        """
        增加elasticSearch index的统计信息
        :param es_dict:
        :param es_connection:
        :param match_type: 匹配类型，有equal和regex两种，
        :return:
        """
        index_stats_list = []
        index_stats_result = es_connection.indices.stats()
        for index in index_stats_result['indices']:
            if es_dict.get('index') and not self._match_field(index, es_dict.get('index'), match_type):
                continue
            index_stats_list.append(dict({'index': index}, **index_stats_result['indices'][index]['primaries']))
        return index_stats_list

    def _merge_actual_doc_num(self, index_type_dict_list, es_connection):
        """
        增加实际文档数目，不包含子文档
        :param index_type_dict_list:
        :param es_connection:
        :return:
        """
        count_dsl_list = map(lambda index_type_dict: (
            {"search_type": "count", "index": index_type_dict["index"], "doc_type": index_type_dict["type"]},
            {"query": {"match_all": {}}}), index_type_dict_list)
        count_body = chain(*count_dsl_list)
        es_count_result_list = es_connection.msearch(count_body)
        doc_count_list = map(lambda count_result: count_result['hits']['total'], es_count_result_list['responses'])
        for (index_type_dict, doc_count) in zip(index_type_dict_list, doc_count_list):
            index_type_dict['count'] = doc_count
        return index_type_dict_list

    def _get_default_es_host(self):
        """
        获取ES默认服务器，采用云销商品的地址作为ES默认地址
        """
        yun_shop_es_cfg = config.get_value('/es_index_setting/product')
        return yun_shop_es_cfg['host']


class Shop(EsIndex):
    """
    商店操作接口
    """

    def add_shop(self, admin_id):
        """
        增加商店
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        shop_es_config = es_adapter.get_product_es_cfg(admin_id)
        self.add_index(shop_es_config)

    def delete_shop(self, admin_id):
        """
        删除商店
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        shop_es_config = es_adapter.get_product_es_cfg(admin_id)
        self.delete_type(shop_es_config)

    def delete_all_shop_products(self, admin_id):
        """
        删除商店所有商品接口
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        shop_es_config = es_adapter.get_product_es_cfg(admin_id)
        self.delete_all_index_docs(shop_es_config)

    def query_shops(self, admin_id=None):
        """
        查询商店信息
        :param admin_id:
        :return:
        """
        return self.query_es_index_info_list(es_adapter.get_product_es_cfg(admin_id), 'equal' if admin_id else 'regex')


class EsDoc(object):
    """
    ES文档管理
    """

    def query(self, es_cfg, query_params):
        """
        查找文档数据
        :param es_cfg:
        :param query_params:
        :return:
        """
        if 'host' not in es_cfg:
            es_cfg['host'] = self._get_default_es_host()
        return SearchPlatformDoc.objects.get(es_cfg, es_cfg['index'], es_cfg['type'], query_params)

    def add(self, es_cfg, data_list):
        if 'host' not in es_cfg:
            es_cfg['host'] = self._get_default_es_host()
        return es_adapter.batch_create(es_cfg, data_list)

    def update(self, es_cfg, data_list):
        if 'host' not in es_cfg:
            es_cfg['host'] = self._get_default_es_host()
        return es_adapter.batch_update(es_cfg, data_list)

    def delete(self, request):
        def build_doc_delete_body(es_config, doc_id=None, doc=None):
            """
            构建单个文档批量删除数据结构
            :param es_config:
            :param doc_id:
            :param doc:
            :return:
            """
            index, es_type, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
            return {"delete": {"_index": index, "_type": es_type, "_id": doc_id}}

        def build_batch_delete_body(es_config, doc_list):
            """
            构造商品列表批量删除的ES数据结构，,此处可以优化，不需要每次都获取index和type
            :param es_config:
            :param doc_list:
            :return:
            """
            if not doc_list:
                return []
            return chain(map(lambda doc: build_doc_delete_body(es_config, doc=doc), doc_list))

        es_config = self._get_es_config(request.QUERY_PARAMS)
        doc_list = []
        if 'data_list' in request.DATA:
            doc_list = request['data_list']
        elif 'data' in request.DATA:
            doc_list = [request['data']]
        bulk_delete_body = build_batch_delete_body(es_config, doc_list)
        es_connection = EsConnectionFactory.get_es_connection(host=es_config['host'])
        return es_connection.bulk(bulk_delete_body)

    def delete_by_id(self, es_cfg, doc_id):
        if 'host' not in es_cfg:
            es_cfg['host'] = self._get_default_es_host()
        bulk_delete_body = [{"delete": {"_index": es_cfg['index'], "_type": es_cfg['type'], "_id": doc_id}}]
        es_connection = EsConnectionFactory.get_es_connection(host=es_cfg['host'])
        es_result = es_connection.bulk(bulk_delete_body)
        return es_adapter.process_es_bulk_result(es_result)

    def delete_by_query(self, es_cfg, query_param):
        if 'host' not in es_cfg:
            es_cfg['host'] = self._get_default_es_host()
        es_connection = EsConnectionFactory.get_es_connection(host=es_cfg['host'])
        dsl = SearchPlatformDoc.objects.get_dsl(es_cfg, es_cfg['index'], es_cfg['type'], query_param, query_param,
                                                es_connection)
        return es_adapter.delete_by_query(es_cfg, {}, dsl)

    def _get_default_es_host(self):
        """
        获取ES默认服务器，采用云销商品的地址作为ES默认地址
        """
        yun_shop_es_cfg = config.get_value('/es_index_setting/product')
        return yun_shop_es_cfg['host']

    def _get_es_config(self, params):
        """
        获取ES配置参数
        :param params:
        :return:
        """
        if 'reference' in params:
            es_config = config.get_value('es_index_setting/' + params['reference'])
            es_config = merge(es_config, params)
            assert es_config, 'the reference is not exist, reference={0}'.format(params)
        else:
            es_config = dict(params)
        index, doc_type, doc_id = es_adapter.get_es_doc_keys(es_config, kwargs=params)
        es_config['index'] = index
        es_config['type'] = doc_type
        es_config['id'] = doc_id

        if 'host' not in es_index:
            es_config['host'] = self._get_default_es_host()
        return es_config


class ShopProduct(EsDoc):
    """
    商品管理
    """

    def query(self, query_params, admin_id):
        if not admin_id:
            return

        es_config = es_adapter.get_product_es_cfg(admin_id)
        return Product.objects.get(es_config, es_config['index'], es_config['type'], query_params)

    def add(self, data, admin_id):
        if not admin_id:
            return

        es_config = es_adapter.get_product_es_cfg(admin_id)
        doc_list = data
        if not isinstance(data, (list, tuple, set)):
            doc_list = [data]

        return es_adapter.batch_create(es_config, doc_list)

    def delete(self, query_params, admin_id):
        if not admin_id:
            return

        es_config = es_adapter.get_product_es_cfg(admin_id)
        return self.delete_by_query(es_config, query_params)

    def delete_by_id(self, query_params, admin_id, doc_id):
        if not admin_id:
            return

        es_config = es_adapter.get_product_es_cfg(admin_id)
        return super(ShopProduct, self).delete_by_id(es_config, doc_id)


    def update(self, data, admin_id):
        if not admin_id:
            return

        es_config = es_adapter.get_product_es_cfg(admin_id)
        doc_list = data
        if not isinstance(data, (list, tuple, set)):
            doc_list = [data]

        return es_adapter.batch_update(es_config, doc_list)
class VipAdminId(object):
    def __init__(self):
        self.host = SERVICE_BASE_CONFIG.get('redis_admin_id_config') or SERVICE_BASE_CONFIG.get('msg_queue')
        self.redis_conn = RedisConnectionFactory.get_redis_connection(self.host)
        self.vip_users_key = config.get_value(
            '/consts/global/admin_id_cfg/vip_id_key') or 'search_platform_vip_admin_id_set'
        sp_shop_init_rivers = filter(
            lambda item: item['notification'].get('queue', '') == 'q.search_platform.shopInit',
            config.get_value('/data_river/rivers'))
        self.sp_shop_init_river = sp_shop_init_rivers[0] if sp_shop_init_rivers else {}
        self.sp_shop_init_river_key = get_river_key(self.sp_shop_init_river)
    def delete(self, admin_id):
        """
        将admin 用户降级为非VIP用户
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        self.redis_conn.srem(self.vip_users_key, *admin_id.split(','))
        self.send_update_msg()
    def add(self, admin_id):
        """
        将admin用户添加为VIP用户
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        self.redis_conn.sadd(self.vip_users_key, *admin_id.split(','))
        self.send_update_msg()
    def upgrade_vip(self, admin_id):
        """
        将用户从体验用户升级为VIP，
        首先初始化用户数据到VIP集群，通过同步执行‘q.search_platform.shopInit’消息数据流，
        初始化成功后再将用户admin id添加到集群中
        :param admin_id:
        :return:
        """
        if not admin_id:
            return
        if self.redis_conn.sismember(self.vip_users_key, admin_id):
            return
        try:
            sys = 0 if admin_id == 'A000000' else 2
            message_text = '"chainMasterId":"{0}","sys":{1}'.format(admin_id, sys)
            message_text = '{' + message_text + '}'
            msg = {'type': 'pyactivemq.TextMessage', 'text': message_text, 'adminId': admin_id, 'redo': False,
                   'river_key': self.sp_shop_init_river_key}
            process_syn_message(msg, self.sp_shop_init_river_key)
            self.add(admin_id)
        except Exception as e:
            app_log.error('upgrade_vip fail {0}', e, admin_id)
    def query(self, admin_ids=None):
        """
        查询所有VIP用户
        :return:
        """
        vip_admin_ids = self.redis_conn.smembers(self.vip_users_key)
        if admin_ids:
            admin_id_list = admin_ids.split(',')
            intersection_list = [admin_id.strip() for admin_id in admin_id_list if admin_id.strip() in vip_admin_ids]
            return None if not intersection_list else intersection_list
        return sorted(vip_admin_ids)
    def send_update_msg(self):
        """
        发送VIP用户变更消息
        :return:
        """
        message_bus.publish(Event.TYPE_VIP_ADMIN_ID_UPDATE, source='', body='')
class Cluster():
    def __init__(self):
        message_bus.add_event_listener(Event.TYPE_REST_CLUSTER_UPDATE_CONFIG, self.update_config)
        self._msg_redis_host = settings.SERVICE_BASE_CONFIG.get('msg_queue')
        self._msg_redis_conn = RedisConnectionFactory.get_redis_connection(self._msg_redis_host)
        self._final_msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_final_queue_key') or "sp_msg_final_queue"
        self._redo_msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_redo_queue_key') or "sp_msg_redo_queue_{0}"
        self._msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_queue_key') or "sp_msg_queue_{0}"
    def fail_over_es(self, data):
        """
        将ES切换到备份服务器上
        :param data:
        :return:
        """
        def set_vip_es_host(_body):
            back_vip_es_host = config.get_value('/consts/custom_variables/back_vip_es_host')
            _body['/consts/custom_variables/vip_es_host'] = back_vip_es_host
        def set_experience_es_host(_body):
            back_experience_es_host = config.get_value('/consts/custom_variables/back_experience_es_host')
            _body['/consts/custom_variables/experience_es_host'] = back_experience_es_host
        target = data.get('target')
        body = {}
        if target == 'vip':
            set_vip_es_host(body)
        elif target == 'experience':
            set_experience_es_host(body)
        elif target == 'both':
            set_experience_es_host(body)
            set_vip_es_host(body)
        if body:
            message_bus.publish(Event.TYPE_REST_CLUSTER_UPDATE_CONFIG, source='',
                                body={'update_values': body, 'operation': 'update'})
    def fail_back_es(self, data=None):
        """
        将ES切换回正式服务器
        :param data:
        :return:
        """
        message_bus.publish(Event.TYPE_REST_CLUSTER_UPDATE_CONFIG, source='', body={'operation': 'reload'})
    def update_config(self, event):
        """
        根据config配置更新消息处理函数
        :param event:
        :return:
        """
        app_log.info('Cluster update config is called {0}', event)
        if not event.data:
            app_log.info('Cluster update config event has no data')
            return
        if event.data.get('operation') == 'update':
            update_values = event.data.get('update_values', {})
            app_log.info('Cluster update config {0}', update_values)
            for item_key, item_value in update_values.iteritems():
                config.update_value(item_key, item_value)
        elif event.data.get('operation') == 'reload':
            config_holder.synchronize_config('file', 'cache', True)
    def operate_rest_qos_processor(self, operation='stop'):
        """
        处理restful接口失败消息重做程序启停
        :param operation 目前支持 'start' 和 'stop'
        :return:
        """
        app_log.info('Cluster operate rest qos processor is called')
        message_bus.publish(Event.TYPE_REST_KAFKA_CONSUMER_START_STOP, source='', body={'operation': operation})
    def get_msg_queue(self, admin_id, start=0, size=0):
        """
        获取消息队列信息，如果size参数大于0，则返回size条消息记录
        :param admin_id:
        :param start:
        :param size:
        :return:
        """
        if not admin_id:
            raise InvalidParamError('Admin ID cannot be null')
        admin_msg_queue_key = self._msg_queue_key.format(admin_id)
        return self._get_redis_list_info(admin_msg_queue_key, start, size)
    def delete_msg_queue(self, admin_id):
        """
        删除消息队列信息
        :param admin_id:
        :return:
        """
        if not admin_id:
            raise InvalidParamError('Admin ID cannot be null')
        admin_msg_queue_key = self._msg_queue_key.format(admin_id)
        return self._msg_redis_conn.delete(admin_msg_queue_key)
    def get_redo_msg_queue(self, admin_id, start=0, size=0):
        """
        获取用户重做消息队列信息
        :param admin_id:
        :param start:
        :param size:
        :return:
        """
        if not admin_id:
            raise InvalidParamError('Admin ID cannot be null')
        admin_msg_queue_key = self._redo_msg_queue_key.format(admin_id)
        return self._get_redis_list_info(admin_msg_queue_key, start, size)
    def delete_redo_msg_queue(self, admin_id):
        """
        删除用户重做队列信息
        :param admin_id:
        :return:
        """
        if not admin_id:
            raise InvalidParamError('Admin ID cannot be null')
        admin_msg_queue_key = self._redo_msg_queue_key.format(admin_id)
        return self._msg_redis_conn.delete(admin_msg_queue_key)
    def get_final_msg_queue(self, start=0, size=0):
        """
        获取最终失败消息队列信息
        :param start:
        :param size:
        :return:
        """
        return self._get_redis_list_info(self._final_msg_queue_key, start, size)
    def delete_final_msg_queue(self):
        """
        删除最终失败消息队列信息
        :return:
        """
        return self._msg_redis_conn.delete(self._final_msg_queue_key)
    def _get_redis_list_info(self, list_key, start, size):
        """
        获取redis list信息，如果size参数大于0，则返回size条记录
        :param list_key:
        :param start:
        :param size:
        :return:
        """
        queue_size = self._get_redis_list_size(list_key)
        result = {'total': queue_size}
        if size > 0:
            msg_str_list = self._get_redis_list_range(list_key, start, size - 1)
            msg_list = map(lambda msg_str: json.loads(msg_str), msg_str_list)
            result['root'] = msg_list
        return result
    def _get_redis_list_size(self, list_key):
        """
        获取redis队列长度
        :param list_key:
        :return:
        """
        return self._msg_redis_conn.llen(list_key)
    def _get_redis_list_range(self, list_key, start=0, size=4):
        """
        获取redis队列范围
        :param list_key:
        :param start:
        :param size:
        :return:
        """
        return self._msg_redis_conn.lrange(list_key, start, start + size)
    def get_rest_request_queue(self):
        """
        获取REST请求失败队列信息, kafka python接口无法获取topic offset，通过kafka manager 接口获取html进行解析
        :return:
        """
        kafka_manager_host = config.get_value('/consts/custom_variables/kafka_manager_host')
        kafka_manager_cluster_name = config.get_value('/consts/query/sla/kafka_manager_cluster_name')
        kafka_consumer_group = config.get_value('/consts/query/sla/rest_request_fail_consumer_redo_group')
        if not kafka_manager_host:
            return None
        url = kafka_manager_host + '/clusters/' + kafka_manager_cluster_name + '/consumers/' + kafka_consumer_group + '/type/KF'
        response = urllib2.urlopen(url, timeout=5)
        content = response.read(30000)
        _, str_offset_lag = unbind_variable(r'<td>[ ]*(?P<offset_lag>[\d]+)[ ]*</td>', 'offset_lag', content)
        return {'total': int(str_offset_lag) if str_offset_lag else 0}


SUPERVISOR_PROXY_CACHE = {}

supervisor = Supervisor()
data_river = DataRiver()
query_chain = QueryChain()
sys_param = SystemParam()
es_tmpl = EsTmpl()
message = Message()
ansjSegmentation = AnsjSegmentation()
suggest = Suggest()
es_index = EsIndex()
shop = Shop()
shop_product = ShopProduct()
es_doc = EsDoc()
vip_admin_id_model = VipAdminId()
cluster = Cluster()

if __name__ == '__main__':


    # proxy = xmlrpclib.ServerProxy('http://localhost:9001/RPC2')
    # supervisor = proxy.supervisor
    # if not supervisor:
    # print supervisor.getState()
    # print supervisor.getPID()
    # print supervisor.getAllProcessInfo()
    # print supervisor.readLog(-300, 0)
    print cluster.get_rest_request_queue()
