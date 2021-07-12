# -*- coding: utf-8 -*-
from collections import OrderedDict
import time
from itertools import chain
import ujson as json

import re
from elasticsearch import ElasticsearchException
from elasticsearch import helpers
import elasticsearch7

from common.admin_config import admin_config
from common.es_routers import es_router
from common.exceptions import EsBulkOperationError
from common.utils import get_dict_value_by_path, bind_variable, bind_dict_variable, get_default_es_host, upper_admin_id
from common.loggers import app_log, debug_log
from common.configs import config
from common.connections import EsConnectionFactory, Es7ConnectionFactory
from search_platform.settings import SERVICE_BASE_CONFIG

__author__ = 'liuzhaoming'

BATCH_REQUEST_TIMEOUT = config.get_value('consts/global/es_conn_param/batch_request_timeout') or 30
BATCH_TIMEOUT = config.get_value('consts/global/es_conn_param/batch_timeout') or 120000
INDEX_REQUEST_TIMEOUT = config.get_value('consts/global/es_conn_param/index_request_timeout') or 120
INDEX_TIMEOUT = config.get_value('consts/global/es_conn_param/index_timeout') or 120000


class Es7IndexAdapter(object):
    """
    ES7索引数据适配器
    """

    def batch_create(self, es_config, doc_list, input_param=None):
        """
        批量创建或者更新文档，如果是更新文档，需要带上所有属性，
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not doc_list:
            return
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or doc_list[0])
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        bulk_body = self.__build_batch_create_body(es_config, doc_list=doc_list, input_index=index)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': '{}ms'.format(BATCH_TIMEOUT)})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_update(self, es_config, doc_list, input_param=None):
        """
        批量更新文档，只需要给定要更新的字段即可
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not doc_list:
            return
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or doc_list[0])
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        bulk_body = self.__build_batch_update_body(es_config, doc_list=doc_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': '{}ms'.format(BATCH_TIMEOUT)})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_update_with_props_by_ids(self, es_config, doc_list, input_param=None, id_separator=','):
        """
        根据给定的ID批量更新某个或某几个属性
        doc的格式为：{ids:"1,2,3,4", data:{prop1:value1, prop2:value2}, adminId:a12000}
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :param id_separator
        :return:
        """
        if not doc_list:
            return
        bulk_body = []
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        for doc in doc_list:
            index, doc_id = self.get_es_doc_keys(es_config, kwargs=dict(input_param or {}, **doc))
            if 'ids' not in doc or 'data' not in doc:
                continue
            id_list = doc['ids'].split(id_separator)
            map(lambda _doc_id: bulk_body.extend(
                ({"update": {"_index": index, "_id": _doc_id}}, {"doc": doc['data']})), id_list)

        try:
            es_connection = Es7ConnectionFactory.get_es_connection(
                es_config=dict(es_config, index=index, version=config.get_value('version')))
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': '{}ms'.format(BATCH_TIMEOUT)})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_delete(self, es_config, request_param, input_param):
        """
        批量删除文档
        :param es_config:
        :param request_param:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not request_param:
            return
        if isinstance(request_param, tuple) or isinstance(request_param, list):
            request_param = request_param[0]

        es_config = es_router.route(es_config, input_param=input_param or request_param)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or request_param)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        separator = es_config['id_process']['separator'] if 'id_process' in es_config and 'separator' in es_config[
            'id_process'] and es_config['id_process']['separator'] else ':'
        ids_str = bind_variable(es_config['id'], request_param)
        if not ids_str:
            app_log.error("Cannot find ids request_param:{0}", request_param)

        doc_id_list = ids_str.strip().strip(';').split(separator)
        bulk_body = self.__build_batch_delete_body_by_ids(index, doc_id_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': '{}ms'.format(BATCH_TIMEOUT)})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_delete_by_ids(self, es_config, doc_ids, message_parse_result={}, separator=':'):
        if not doc_ids:
            return
        doc_id_list = doc_ids.strip().strip(';').split(separator)
        es_config = es_router.route(es_config, input_param=message_parse_result)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=message_parse_result)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        bulk_body = self.__build_batch_delete_body_by_ids(index, doc_id_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': '{}ms'.format(BATCH_TIMEOUT)})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('es operation input param is {0}', e, list(bulk_body))
            raise e

    def delete_all_doc(self, es_config, doc):
        """
        删除type中得所有文档
        :param es_config:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        try:
            es_connection.delete_by_query(index=index, body={'query': {'match_all': {}}},
                                          params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                  'timeout': '{}ms'.format(INDEX_TIMEOUT)})
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('es delete_all_doc input param is {0}', e, index)

    def delete_all_doc_by_index(self, es_config, doc):
        """
        通过删除type来删除所有文档，效率较高
        :param es_config:
        :param doc:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')))
        try:
            es_connection.indices.delete_mapping(index=index,
                                                 params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                         'timeout': '{}ms'.format(INDEX_TIMEOUT)})
            es_connection.indices.put_mapping(index=index, body=es_config['mapping'],
                                              ignore_conflicts=True, params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                                             'timeout': '{}ms'.format(INDEX_TIMEOUT)})
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('es delete_all_doc_by_index input param is {0}', e, index)

    def delete_by_query(self, es_config, doc, body=None):
        """
        根据ES query dsl 删除符合条件的文档
        :param es_config:
        :param doc:
        :param body:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, version=config.get_value('version')),
            create_index=False)
        try:
            body = body if body else {'query': {'match_all': {}}}
            if 'size' in body:
                del body['size']
            if 'from' in body:
                del body['from']
            es_connection.delete_by_query(index=index, body=body,
                                          params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                  'timeout': '{}ms'.format(BATCH_TIMEOUT)})
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('es delete_by_query input param is {0}', e, index)

    def delete_by_field(self, es_config, doc, field_name, field_value_list):
        """
        根据指定的字段值删除符合条件的文档
        :param es_config:
        :param doc
        :param field_name:
        :param field_value_list:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = Es7ConnectionFactory.get_es_connection(
            es_config=dict(es_config, version=config.get_value('version')),
            create_index=False)
        try:
            body = {'query': {'terms': {
                field_name: field_value_list if isinstance(field_value_list, (set, tuple, list))
                else [field_value_list]}}}
            es_connection.delete_by_query(index=index, body=body,
                                          params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                  'timeout': '{}ms'.format(BATCH_TIMEOUT)})
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('es delete_by_field error input param is {0}, {1}, {2}', e, index, field_name,
                          field_value_list)

    def get_es_doc_keys(self, es_config, doc_id=None, kwargs=None):
        """
        获取ES 文档的 index，id
        :param es_config:
        :param doc_id:
        :param kwargs:
        :return:
        """
        index_template = get_dict_value_by_path('index', es_config)
        id_template = get_dict_value_by_path('id', es_config)
        version = config.get_value('version')
        params = dict(kwargs if kwargs else {}, **{'version': version})
        index = bind_variable(index_template, params)
        doc_id = doc_id if doc_id else bind_variable(id_template, params)
        return index, doc_id

    def query_docs(self, query_body, host, index, doc_type=None, params={}):
        """
        查询ES索引库中的数据
        :param query_body:
        :param host:
        :param index:
        :param doc_type:
        :param params:
        :return:
        """
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        return es_connection.search(index=index, body=query_body, doc_type=doc_type, params=params)

    def get_fields_mapping(self, host, index, doc_type, field_list):
        """
        获取ES mapping数据
        :param host:
        :param index:
        :param doc_type:
        :return:
        """
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        return es_connection.indices.get_field_mapping(field_list, index, doc_type)

    def query_text_analyze_result(self, host, analyzer, text, keyword_filter_regex, request_param):
        """
        对文本进行分词,支持过滤
        :param analyzer:
        :param text:
        :param keyword_filter_regex:
        :return:
        """
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        analyze_result = es_connection.indices.analyze(index=request_param['index'],
                                                       body={'analyzer': analyzer, 'text': text})
        return [ele['token'] for ele in analyze_result['tokens'] if
                re.search(keyword_filter_regex, str(ele['token'])) and len(ele['token']) > 1]

    def query_text_analyze_result_without_filter(self, es_connection, analyzer, text, index=None, host=None):
        """
        对文本进行分词,不支持过滤
        :param es_connection:
        :param analyzer:
        :param text:
        :return:
        """
        if not es_connection:
            es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        analyze_result = es_connection.indices.analyze(index=index, params={'analyzer': analyzer, 'text': text})
        return list(set([ele['token'] for ele in analyze_result['tokens'] if len(ele['token']) > 0]))

    def multi_search(self, body, host, index=None, doc_type=None):
        """
        批量搜索
        :param body:
        :param host:
        :param index:
        :param doc_type:
        :return:
        """
        from elasticsearch7.client.utils import SKIP_IN_PATH, _make_path, _bulk_body

        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")
        data = es_connection.transport.perform_request('POST', _make_path(index, '_msearch'),
                                                       body=_bulk_body(es_connection.transport.serializer, body),
                                                       params={'request_timeout': BATCH_REQUEST_TIMEOUT})
        return data

    @debug_log.debug('Save config')
    def save_config(self, config_data):
        """
        保存meta数据
        :param config_data:
        :param version:
        :return:
        """
        app_log.info('Save config is called {0}', config_data)
        if not config_data:
            return

        default_config_data = config_data
        es_connection = Es7ConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        update_body = []
        for key in default_config_data:
            update_body.append({"update": {"_index": SERVICE_BASE_CONFIG.get('meta_es_index'),
                                           "_type": SERVICE_BASE_CONFIG.get('meta_es_type'), "_id": key}})
            if key == 'version':
                update_body.append({'doc': {'version': default_config_data[key]}})
            else:
                update_body.append({'doc': {'json_str': json.dumps(default_config_data[key],
                                                                   sort_keys=True if key == 'data_river' else False)}})
        try:
            es_bulk_result = es_connection.bulk(update_body, params={'wait_for_completion': True})
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation fail input param is {0}', e, list(update_body))

    @debug_log.debug('Insert config')
    def insert_config(self, config_data):
        """
        保存meta数据,原有的会被清除
        :param config_data:
        :param version:
        :return:
        """
        app_log.info('Insert config is called {0}', config_data)
        if not config_data:
            return

        default_config_data = config_data
        es_connection = Es7ConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        update_body = []
        for key in default_config_data:
            update_body.append({"index": {"_index": SERVICE_BASE_CONFIG.get('meta_es_index'),
                                          "_type": SERVICE_BASE_CONFIG.get('meta_es_type'), "_id": key}})
            if key == 'version':
                update_body.append({'version': default_config_data[key]})
            else:
                update_body.append({'json_str': json.dumps(default_config_data[key],
                                                           sort_keys=True if key == 'data_river' else False)})
        try:
            es_bulk_result = es_connection.bulk(update_body)
            return self.process_es_bulk_result(es_bulk_result)
        except elasticsearch7.ElasticsearchException as e:
            app_log.error('ES operation fail input param is {0}', e, list(update_body))

    def get_config(self, key=None):
        """
        从ES读取Meta文件
        :param key:
        :return:
        """
        app_log.info('Get config is called')
        es_connection = Es7ConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        es_result = es_connection.search(index=SERVICE_BASE_CONFIG.get('meta_es_index'),
                                         doc_type=SERVICE_BASE_CONFIG.get('meta_es_type'))
        doc_list = es_result['hits'].get('hits')
        if not doc_list:
            return {}
        config_data = {}
        for doc in doc_list:
            if doc['_id'] == 'version':
                field_value = doc['_source']['version']
            else:
                field_value = json.loads(doc['_source']['json_str'], object_pairs_hook=OrderedDict) \
                    if 'json_str' in doc['_source'] else {}

            config_data[doc['_id']] = field_value
        if key:
            return {key: config_data[key]} if key in config_data else {}
        return config_data

    def search(self, query_body, host, index, doc_type=None, params={}):
        """
        查询ES索引库中的数据
        :param query_body:
        :param host:
        :param index:
        :param doc_type:
        :param params:
        :return:
        """
        es_result = self.query_docs(index=index, query_body=query_body, params=params, host=host)
        if not es_result:
            return {'root': [], 'total': 0}
        return {'root': map(lambda item: item.get('_source'), es_result['hits'].get('hits')),
                'total': es_result['hits'].get('total')}

    def create_doc(self, host, index, doc_type, doc):
        """
        创建文档
        :param host:
        :param index:
        :param doc_type:
        :param doc:
        :return:
        """
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        return es_connection.create(index, doc_type, doc, params={'wait_for_completion': True})

    def get_product_es_cfg(self, admin_id):
        """
        获取用户的ES相关配置
        :param admin_id:
        :return:
        """
        variable_values = {'adminId': admin_id if admin_id else '[\\d\\D]+?', 'version': config.get_value('version')}
        if admin_id == 'gonghuo':
            sup_shop_es_cfg = config.get_value('/es_index_setting/gonghuo_product')
            return bind_dict_variable(sup_shop_es_cfg, variable_values)
        else:
            yun_shop_es_cfg = config.get_value('/es_index_setting/product')
            return bind_dict_variable(yun_shop_es_cfg, variable_values)

    def completion_suggest(self, query_body, host, index):
        """
        ES completion_suggest查询
        :param query_body:
        :param host:
        :param index:
        :return:
        """
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        return es_connection.suggest(query_body, index)

    def scroll(self, scroll_time=None, body=None, search_type=None, scroll_id=None, **es_cfg):
        """
        scroll查询
        :param scroll_time:
        :param search_type:
        :param scroll_id:
        :param es_cfg:
        :return:
        """
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        scroll_time = scroll_time or config.get_value('/consts/query/scroll_time')
        first_run = True if not scroll_id else False
        search_params = {'index': es_cfg['index']}
        search_type = search_type or 'query_then_fetch'
        search_params['search_type'] = search_type
        if first_run:
            if 'from' in body:
                # 在新版本的es中，scroll不允许包含from参数
                # https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html#_scroll
                del body['from']
            resp = es_connection.search(body=body, scroll=scroll_time, **search_params)
        else:
            resp = es_connection.scroll(scroll_id=scroll_id, scroll=scroll_time)
        return resp

    def scan(self, scroll_time=None, body=None, preserve_order=False, es_search_params=None, **es_cfg):
        """
        scan查询，会查询出所有符合查询条件的数据，默认不进行排序
        ⚠ scan查询在es5.0之后的版本中已经被废弃
        参见：https://www.elastic.co/guide/en/elasticsearch/reference/5.1/breaking_50_search_changes.html#_literal_search_type_scan_literal_removed
        :param scroll_time:
        :param body:
        :param es_cfg:
        :return:
        """
        # scan查询已经废弃，使用scroll查询进行替代
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)

        search_params = {'index': es_cfg['index'], 'search_type': 'query_then_fetch'}
        # 当body中没有设置sort字段时，设置默认的sort为_doc
        if 'sort' not in body:
            # 在新版本的ES中scroll使用_doc排序可以得到和旧版本中scan一样的效果
            body['sort'] = ['_doc']
        return es_connection.search(body=body, scroll=scroll_time, **search_params)

    def delete_scroll(self, scroll_ids, **es_cfg):
        """
        手工删除scroll缓存
        :param scroll_ids:
        :param es_cfg:
        :return:
        """
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = Es7ConnectionFactory.get_es_connection(host=host)
        return es_connection.custom_clear_scroll({
            'scroll_id': scroll_ids.strip().split(',')
        })

    def get_spu_es_setting(self, admin_id):
        """
        获取spu ES 设置
        :param admin_id:
        :return:
        """
        spu_setting_key = '/es_index_setting/spu_vip' if admin_config.is_vip(
            admin_id) else '/es_index_setting/spu_experience'
        spu_es_setting = config.get_value(spu_setting_key)
        params = {'adminId': admin_id, 'version': config.get_value('version')}
        return {'index': bind_variable(spu_es_setting.get('index'), params),
                'type': bind_variable(spu_es_setting.get('type'), params),
                'id': bind_variable(spu_es_setting.get('id'), params)}

    def get_sku_es_setting(self, admin_id):
        """
        获取sku ES 设置
        :param admin_id:
        :return:
        """
        sku_setting_key = '/es_index_setting/product_vip' if admin_config.is_vip(
            admin_id) else '/es_index_setting/product_experience'
        sku_es_setting = config.get_value(sku_setting_key)
        params = {'adminId': admin_id, 'version': config.get_value('version')}
        return {'index': bind_variable(sku_es_setting.get('index'), params),
                'type': bind_variable(sku_es_setting.get('type'), params),
                'id': bind_variable(sku_es_setting.get('id'), params)}

    def __build_batch_create_body(self, es_config, doc_list, input_index=None):
        """
        构造商品列表批量更新的ES数据结构，,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :param input_index
        :return:
        """
        if not doc_list:
            return []
        return chain(*map(lambda doc: self.__build_doc_create_body(es_config, doc=doc, input_index=input_index,
                                                                   ), doc_list))

    def __build_batch_update_body(self, es_config, doc_list, input_index=None):
        """
        构造商品列表批量更新的ES数据结构，只需要给定要更新的字段即可,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :param input_index:
        :return:
        """
        if not doc_list:
            return []
        return chain(
            *map(lambda doc: self.__build_doc_update_body(es_config, doc=doc, input_index=input_index), doc_list))

    def __build_batch_delete_body(self, es_config, doc_list):
        """
        构造商品列表批量删除的ES数据结构，,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return []
        return chain(map(lambda doc: self.__build_doc_delete_body(es_config, doc=doc), doc_list))

    def __build_batch_delete_body_by_ids(self, es_index, doc_id_list):
        """
        根据doc_id列表构造批量删除ES数据结构
        :param es_index:
        :param doc_id_list:
        :return:
        """
        return [self.__build_doc_delete_body_by_id(es_index, doc_id) for doc_id in doc_id_list if doc_id]

    def __build_doc_create_body(self, es_config, doc, input_index=None):
        """
        构建单个文档的批量创建数据结构，如果文档已经存在，则为更新操作,无论文档是否存在，都需要提供文档全量数据
        :param es_config:
        :param doc:
        :param input_index: 如果已经计算好input_index,直接使用
        :return:
        """
        if not doc:
            return ()
        index, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        return {"index": {"_index": input_index or index, "_id": doc_id}}, doc

    def __build_doc_update_body(self, es_config, doc_id=None, doc=None, input_index=None):
        """
        构建单个文档批量更新数据结构,文本必须存在，不需要提供全量数据
        :param es_config:
        :param doc_id:
        :param doc:
        :param input_index
        :return:
        """
        if not doc:
            return ()
        index, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"update": {"_index": input_index or index, "_id": doc_id}}, {"doc": doc}

    def __build_doc_delete_body(self, es_config, doc_id=None, doc=None, input_index=None):
        """
        构建单个文档批量删除数据结构
        :param es_config:
        :param doc_id:
        :param doc:
        :return:
        """
        index, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"delete": {"_index": input_index or index, "_id": doc_id}}

    def __build_doc_delete_body_by_id(self, es_index, doc_id):
        return {"delete": {"_index": es_index, "_id": doc_id}}

    def process_es_bulk_result(self, bulk_result):
        """
        处理ES bulk操作结果
        :param bulk_result:
        {u'items':
            [{u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }},
            {u'update': {u'status': 404, u'_type': u'QmShopProduct', u'_id': u'20', u'error':
                          u'DocumentMissingException[[seach_test][-1] [QmShopProduct][20]: document missing]',
                          u'_index': u'seach_test'}},
            {u'index': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'301', u'_version': 2,
                        u'_index': u'seach_test'}}
            ],
        u'errors': True, u'took': 6}
        :return:
        """
        if not bulk_result or not bulk_result.get('errors'):
            # 表示没有错误，所有操作均执行成功
            return
        fail_op_results = filter(self.__is_es_op_fail, bulk_result['items'])
        # 目前对失败的操作只是记录日志，后续可以考虑Redo
        app_log.error('ES bulk operation has errors:{0}', fail_op_results)
        return fail_op_results

    def get_es_bulk_result(self, bulk_result):
        """
        处理ES bulk操作结果
        :param bulk_result:
         {u'items':
            [{u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }},
            {u'update': {u'status': 404, u'_type': u'QmShopProduct', u'_id': u'20', u'error':
                          u'DocumentMissingException[[seach_test][-1] [QmShopProduct][20]: document missing]',
                          u'_index': u'seach_test'}},
            {u'index': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'301', u'_version': 2,
                        u'_index': u'seach_test'}}
            ],
        u'errors': True, u'took': 6}
        :return:
        """
        if not bulk_result or not bulk_result.get('errors'):
            return None
        op_results = map(lambda result_item: not self.__is_es_op_fail(result_item), bulk_result['items'])
        raise EsBulkOperationError(op_results)

    def __is_es_op_fail(self, op_result):
        """
        判断ES操作是否成功
        :param op_result: {u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }}
        :return:
        """
        if not op_result:
            return True
        for key in op_result:
            return False if 'status' in op_result[key] and 200 <= op_result[key]['status'] < 300 else True


es7_adapter = Es7IndexAdapter()


class EsIndexAdapter(object):
    """
    ES索引数据适配器
    """

    def batch_create(self, es_config, doc_list, input_param=None):
        """
        批量创建或者更新文档，如果是更新文档，需要带上所有属性，
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not doc_list:
            return
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or doc_list[0])
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_create_body(es_config, doc_list=doc_list, input_index=index, input_type=doc_type)
        try:
            es_start_time = time.time()
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': BATCH_TIMEOUT})
            # app_log.info('es spend time {0}'.format(time.time() - es_start_time))
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_update(self, es_config, doc_list, input_param=None):
        """
        批量更新文档，只需要给定要更新的字段即可
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not doc_list:
            return
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or doc_list[0])
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_update_body(es_config, doc_list=doc_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': BATCH_TIMEOUT})
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_update_with_props_by_ids(self, es_config, doc_list, input_param=None, id_separator=','):
        """
        根据给定的ID批量更新某个或某几个属性
        doc的格式为：{ids:"1,2,3,4", data:{prop1:value1, prop2:value2}, adminId:a12000}
        :param es_config:
        :param doc_list:
        :param input_param: 上下文解析出来的参数
        :param id_separator
        :return:
        """
        if not doc_list:
            return
        bulk_body = []
        es_config = es_router.route(es_config, input_param=input_param or doc_list[0])
        for doc in doc_list:
            index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=dict(input_param or {}, **doc))
            if 'ids' not in doc or 'data' not in doc:
                continue
            id_list = doc['ids'].split(id_separator)
            map(lambda _doc_id: bulk_body.extend(
                ({"update": {"_index": index, "_type": doc_type, "_id": _doc_id}}, {"doc": doc['data']})), id_list)

        try:
            es_connection = EsConnectionFactory.get_es_connection(
                es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': BATCH_TIMEOUT})
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_delete(self, es_config, request_param, input_param):
        """
        批量删除文档
        :param es_config:
        :param request_param:
        :param input_param: 上下文解析出来的参数
        :return:
        """
        if not request_param:
            return
        if isinstance(request_param, tuple) or isinstance(request_param, list):
            request_param = request_param[0]

        es_config = es_router.route(es_config, input_param=input_param or request_param)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=input_param or request_param)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        separator = es_config['id_process']['separator'] if 'id_process' in es_config and 'separator' in es_config[
            'id_process'] and es_config['id_process']['separator'] else ':'
        ids_str = bind_variable(es_config['id'], request_param)
        if not ids_str:
            app_log.error("Cannot find ids request_param:{0}", request_param)

        doc_id_list = ids_str.strip().strip(';').split(separator)
        bulk_body = self.__build_batch_delete_body_by_ids(index, doc_type, doc_id_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': BATCH_TIMEOUT})
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))
            raise e

    def batch_delete_by_ids(self, es_config, doc_ids, message_parse_result={}, separator=':'):
        if not doc_ids:
            return
        doc_id_list = doc_ids.strip().strip(';').split(separator)
        es_config = es_router.route(es_config, input_param=message_parse_result)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=message_parse_result)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_delete_body_by_ids(index, doc_type, doc_id_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                   'timeout': BATCH_TIMEOUT})
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('es operation input param is {0}', e, list(bulk_body))
            raise e

    def delete_all_doc(self, es_config, doc):
        """
        删除type中得所有文档
        :param es_config:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        try:
            es_connection.delete_by_query(index=index, doc_type=doc_type, body={'query': {'match_all': {}}},
                                          params={'request_timeout': INDEX_REQUEST_TIMEOUT, 'timeout': INDEX_TIMEOUT})
        except ElasticsearchException as e:
            app_log.error('es delete_all_doc input param is {0}, {1}', e, index, doc_type)

    def delete_all_doc_by_type(self, es_config, doc):
        """
        通过删除type来删除所有文档，效率较高
        :param es_config:
        :param doc:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        try:
            es_connection.indices.delete_mapping(index=index, doc_type=doc_type,
                                                 params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                         'timeout': INDEX_TIMEOUT})
            es_connection.indices.put_mapping(index=index, doc_type=doc_type, body=es_config['mapping'],
                                              ignore_conflicts=True, params={'request_timeout': INDEX_REQUEST_TIMEOUT,
                                                                             'timeout': INDEX_TIMEOUT})
        except ElasticsearchException as e:
            app_log.error('es delete_all_doc_by_type input param is {0}, {1}', e, index, doc_type)

    def delete_by_query(self, es_config, doc, body=None):
        """
        根据ES query dsl 删除符合条件的文档
        :param es_config:
        :param doc:
        :param body:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        doc_type = upper_admin_id(doc_type)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')),
            create_index=False)
        try:
            body = body if body else {'query': {'match_all': {}}}
            if 'size' in body:
                del body['size']
            if 'from' in body:
                del body['from']
            es_connection.delete_by_query(index=index, doc_type=doc_type, body=body,
                                          params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                  'timeout': BATCH_TIMEOUT})
        except ElasticsearchException as e:
            app_log.error('es delete_by_query input param is {0}, {1}', e, index, doc_type)

    def delete_by_field(self, es_config, doc, field_name, field_value_list):
        """
        根据指定的字段值删除符合条件的文档
        :param es_config:
        :param doc
        :param field_name:
        :param field_value_list:
        :return:
        """
        es_config = es_router.route(es_config, input_param=doc)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        doc_type = upper_admin_id(doc_type)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')),
            create_index=False)
        try:
            body = {'query': {'terms': {
                field_name: field_value_list if isinstance(field_value_list, (set, tuple, list))
                else [field_value_list]}}}
            es_connection.delete_by_query(index=index, doc_type=doc_type, body=body,
                                          params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                  'timeout': BATCH_TIMEOUT})
        except ElasticsearchException as e:
            app_log.error('es delete_by_field error input param is {0}, {1}, {2}, {3}', e, index, doc_type, field_name,
                          field_value_list)

    def get_es_doc_keys(self, es_config, doc_id=None, kwargs=None):
        """
        获取ES 文档的 index，type，id
        :param es_config:
        :param doc_id:
        :param kwargs:
        :return:
        """
        index_template = get_dict_value_by_path('index', es_config)
        type_template = get_dict_value_by_path('type', es_config)
        id_template = get_dict_value_by_path('id', es_config)
        version = config.get_value('version')
        params = dict(kwargs if kwargs else {}, **{'version': version})
        index = bind_variable(index_template, params)
        doc_type = bind_variable(type_template, params)
        doc_id = doc_id if doc_id else bind_variable(id_template, params)
        return index.lower() if index else index, doc_type if doc_type else doc_type, doc_id

    def query_docs(self, query_body, host, index, doc_type=None, params={}):
        """
        查询ES索引库中的数据
        :param query_body:
        :param host:
        :param index:
        :param doc_type:
        :param params:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        return es_connection.search(index=index, body=query_body, doc_type=doc_type, params=params)

    def get_fields_mapping(self, host, index, doc_type, field_list):
        """
        获取ES mapping数据
        :param host:
        :param index:
        :param doc_type:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        return es_connection.indices.get_field_mapping(field_list, index, doc_type)

    def query_text_analyze_result(self, host, analyzer, text, keyword_filter_regex, request_param):
        """
        对文本进行分词,支持过滤
        :param analyzer:
        :param text:
        :param keyword_filter_regex:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        analyze_result = es_connection.indices.analyze(index=request_param['index'],
                                                       params={'analyzer': analyzer, 'text': text})
        return [ele['token'] for ele in analyze_result['tokens'] if
                re.search(keyword_filter_regex, str(ele['token'])) and len(ele['token']) > 1]

    def query_text_analyze_result_without_filter(self, es_connection, analyzer, text, index=None, host=None):
        """
        对文本进行分词,不支持过滤
        :param es_connection:
        :param analyzer:
        :param text:
        :return:
        """
        if not es_connection:
            es_connection = EsConnectionFactory.get_es_connection(host=host)
        if hasattr(es_connection, 'version') and es_connection.version == '7':
            # 兼容Elasticsearch7版本的analyzer
            analyze_result = es_connection.indices.analyze(index=index, body={'analyzer': analyzer, 'text': text})
        else:
            analyze_result = es_connection.indices.analyze(index=index, params={'analyzer': analyzer, 'text': text})
        return list(set([ele['token'] for ele in analyze_result['tokens'] if len(ele['token']) > 0]))

    def multi_search(self, body, host, index=None, doc_type=None):
        """
        批量搜索
        :param body:
        :param host:
        :param index:
        :param doc_type:
        :return:
        """
        from elasticsearch.client.utils import SKIP_IN_PATH, _make_path

        es_connection = EsConnectionFactory.get_es_connection(host=host)
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")
        _, data = es_connection.transport.perform_request('POST', _make_path(index, doc_type, '_msearch'),
                                                          body=es_connection._bulk_body(body),
                                                          params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                  'timeout': BATCH_TIMEOUT})
        return data

    @debug_log.debug('Save config')
    def save_config(self, config_data):
        """
        保存meta数据
        :param config_data:
        :param version:
        :return:
        """
        app_log.info('Save config is called {0}', config_data)
        if not config_data:
            return

        default_config_data = config_data
        es_connection = EsConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        update_body = []
        for key in default_config_data:
            update_body.append({"update": {"_index": SERVICE_BASE_CONFIG.get('meta_es_index'),
                                           "_type": SERVICE_BASE_CONFIG.get('meta_es_type'), "_id": key}})
            if key == 'version':
                update_body.append({'doc': {'version': default_config_data[key]}})
            else:
                update_body.append({'doc': {'json_str': json.dumps(default_config_data[key],
                                                                   sort_keys=True if key == 'data_river' else False)}})
        try:
            es_bulk_result = es_connection.bulk(update_body, params={'wait_for_completion': True})
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation fail input param is {0}', e, list(update_body))

    @debug_log.debug('Insert config')
    def insert_config(self, config_data):
        """
        保存meta数据,原有的会被清除
        :param config_data:
        :param version:
        :return:
        """
        app_log.info('Insert config is called {0}', config_data)
        if not config_data:
            return

        default_config_data = config_data
        es_connection = EsConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        update_body = []
        for key in default_config_data:
            update_body.append({"index": {"_index": SERVICE_BASE_CONFIG.get('meta_es_index'),
                                          "_type": SERVICE_BASE_CONFIG.get('meta_es_type'), "_id": key}})
            if key == 'version':
                update_body.append({'version': default_config_data[key]})
            else:
                update_body.append({'json_str': json.dumps(default_config_data[key],
                                                           sort_keys=True if key == 'data_river' else False)})
        try:
            es_bulk_result = es_connection.bulk(update_body)
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation fail input param is {0}', e, list(update_body))

    def get_config(self, key=None):
        """
        从ES读取Meta文件
        :param key:
        :return:
        """
        app_log.info('Get config is called')
        es_connection = EsConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        es_result = es_connection.search(index=SERVICE_BASE_CONFIG.get('meta_es_index'),
                                         doc_type=SERVICE_BASE_CONFIG.get('meta_es_type'))
        doc_list = es_result['hits'].get('hits')
        if not doc_list:
            return {}
        config_data = {}
        for doc in doc_list:
            if doc['_id'] == 'version':
                field_value = doc['_source']['version']
            else:
                field_value = json.loads(doc['_source']['json_str'], object_pairs_hook=OrderedDict) \
                    if 'json_str' in doc['_source'] else {}

            config_data[doc['_id']] = field_value
        if key:
            return {key: config_data[key]} if key in config_data else {}
        return config_data

    def search(self, query_body, host, index, doc_type=None, params={}):
        """
        查询ES索引库中的数据
        :param query_body:
        :param host:
        :param index:
        :param doc_type:
        :param params:
        :return:
        """
        es_result = self.query_docs(index=index, query_body=query_body, doc_type=doc_type, params=params, host=host)
        if not es_result:
            return {'root': [], 'total': 0}
        return {'root': map(lambda item: item.get('_source'), es_result['hits'].get('hits')),
                'total': es_result['hits'].get('total')}

    def create_doc(self, host, index, doc_type, doc):
        """
        创建文档
        :param host:
        :param index:
        :param doc_type:
        :param doc:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        return es_connection.create(index, doc_type, doc, params={'wait_for_completion': True})

    def get_product_es_cfg(self, admin_id):
        """
        获取用户的ES相关配置
        :param admin_id:
        :return:
        """
        variable_values = {'adminId': admin_id if admin_id else '[\\d\\D]+?', 'version': config.get_value('version')}
        if admin_id == 'gonghuo':
            sup_shop_es_cfg = config.get_value('/es_index_setting/gonghuo_product')
            return bind_dict_variable(sup_shop_es_cfg, variable_values)
        else:
            yun_shop_es_cfg = config.get_value('/es_index_setting/product')
            return bind_dict_variable(yun_shop_es_cfg, variable_values)

    def completion_suggest(self, query_body, host, index):
        """
        ES completion_suggest查询
        :param query_body:
        :param host:
        :param index:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        return es_connection.suggest(query_body, index)

    def scroll(self, scroll_time=None, body=None, search_type=None, scroll_id=None, **es_cfg):
        """
        scroll查询
        :param scroll_time:
        :param search_type:
        :param scroll_id:
        :param es_cfg:
        :return:
        """
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        scroll_time = scroll_time or config.get_value('/consts/query/scroll_time')
        first_run = True if not scroll_id else False
        search_params = {'index': es_cfg['index'], 'doc_type': es_cfg.get('doc_type') or es_cfg.get('type')}
        search_type = search_type or 'scan'
        search_params['search_type'] = search_type
        if first_run:
            resp = es_connection.search(body=body, scroll=scroll_time, **search_params)
        else:
            resp = es_connection.scroll(scroll_id, scroll=scroll_time)
        return resp

    def scan(self, scroll_time=None, body=None, preserve_order=False, es_search_params=None, **es_cfg):
        """
        scan查询，会查询出所有符合查询条件的数据，默认不进行排序
        :param scroll_time:
        :param body:
        :param es_cfg:
        :return:
        """
        doc_type = es_cfg.get('doc_type') or es_cfg.get('type')
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        es_search_params = es_search_params or {}
        return helpers.scan(es_connection, body, scroll_time, preserve_order, index=es_cfg['index'],
                            doc_type=doc_type, **es_search_params)

    def delete_scroll(self, scroll_ids, **es_cfg):
        """
        手工删除scroll缓存
        :param scroll_ids:
        :param es_cfg:
        :return:
        """
        host = es_cfg.get('host') or get_default_es_host()
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        if scroll_ids:
            scroll_ids = scroll_ids.strip().split(',')
        return es_connection.clear_scroll(scroll_ids)

    def get_spu_es_setting(self, admin_id):
        """
        获取spu ES 设置
        :param admin_id:
        :return:
        """
        spu_setting_key = '/es_index_setting/spu_vip' if admin_config.is_vip(
            admin_id) else '/es_index_setting/spu_experience'
        spu_es_setting = config.get_value(spu_setting_key)
        params = {'adminId': admin_id, 'version': config.get_value('version')}
        return {'index': bind_variable(spu_es_setting.get('index'), params),
                'type': bind_variable(spu_es_setting.get('type'), params),
                'id': bind_variable(spu_es_setting.get('id'), params)}

    def get_sku_es_setting(self, admin_id):
        """
        获取sku ES 设置
        :param admin_id:
        :return:
        """
        sku_setting_key = '/es_index_setting/product_vip' if admin_config.is_vip(
            admin_id) else '/es_index_setting/product_experience'
        sku_es_setting = config.get_value(sku_setting_key)
        params = {'adminId': admin_id, 'version': config.get_value('version')}
        return {'index': bind_variable(sku_es_setting.get('index'), params),
                'type': bind_variable(sku_es_setting.get('type'), params),
                'id': bind_variable(sku_es_setting.get('id'), params)}

    def __build_batch_create_body(self, es_config, doc_list, input_index=None, input_type=None):
        """
        构造商品列表批量更新的ES数据结构，,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :param input_index
        :param input_type
        :return:
        """
        if not doc_list:
            return []
        return chain(*map(lambda doc: self.__build_doc_create_body(es_config, doc=doc, input_index=input_index,
                                                                   input_type=input_type), doc_list))

    def __build_batch_update_body(self, es_config, doc_list, input_index=None, input_type=None):
        """
        构造商品列表批量更新的ES数据结构，只需要给定要更新的字段即可,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :param input_index:
        :param input_type:
        :return:
        """
        if not doc_list:
            return []
        return chain(*map(lambda doc: self.__build_doc_update_body(es_config, doc=doc, input_index=input_index,
                                                                   input_type=input_type), doc_list))

    def __build_batch_delete_body(self, es_config, doc_list):
        """
        构造商品列表批量删除的ES数据结构，,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return []
        return chain(map(lambda doc: self.__build_doc_delete_body(es_config, doc=doc), doc_list))

    def __build_batch_delete_body_by_ids(self, es_index, doc_type, doc_id_list):
        """
        根据doc_id列表构造批量删除ES数据结构
        :param es_index:
        :param doc_type:
        :param doc_id_list:
        :return:
        """
        return [self.__build_doc_delete_body_by_id(es_index, doc_type, doc_id) for doc_id in doc_id_list if doc_id]

    def __build_doc_create_body(self, es_config, doc, input_index=None, input_type=None):
        """
        构建单个文档的批量创建数据结构，如果文档已经存在，则为更新操作,无论文档是否存在，都需要提供文档全量数据
        :param es_config:
        :param doc:
        :param input_index: 如果已经计算好input_index,直接使用
        :param input_type:
        :return:
        """
        if not doc:
            return ()
        index, es_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        return {"index": {"_index": input_index or index, "_type": input_type or es_type, "_id": doc_id}}, doc

    def __build_doc_update_body(self, es_config, doc_id=None, doc=None, input_index=None, input_type=None):
        """
        构建单个文档批量更新数据结构,文本必须存在，不需要提供全量数据
        :param es_config:
        :param doc_id:
        :param doc:
        :param input_index
        :param input_type
        :return:
        """
        if not doc:
            return ()
        index, es_type, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"update": {"_index": input_index or index, "_type": input_type or es_type, "_id": doc_id}}, {"doc": doc}

    def __build_doc_delete_body(self, es_config, doc_id=None, doc=None, input_index=None, input_type=None):
        """
        构建单个文档批量删除数据结构
        :param es_config:
        :param doc_id:
        :param doc:
        :return:
        """
        index, es_type, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"delete": {"_index": input_index or index, "_type": input_type or es_type, "_id": doc_id}}

    def __build_doc_delete_body_by_id(self, es_index, doc_type, doc_id):
        return {"delete": {"_index": es_index, "_type": doc_type, "_id": doc_id}}

    def process_es_bulk_result(self, bulk_result):
        """
        处理ES bulk操作结果
        :param bulk_result:
        {u'items':
            [{u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }},
            {u'update': {u'status': 404, u'_type': u'QmShopProduct', u'_id': u'20', u'error':
                          u'DocumentMissingException[[seach_test][-1] [QmShopProduct][20]: document missing]',
                          u'_index': u'seach_test'}},
            {u'index': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'301', u'_version': 2,
                        u'_index': u'seach_test'}}
            ],
        u'errors': True, u'took': 6}
        :return:
        """
        if not bulk_result or not bulk_result.get('errors'):
            # 表示没有错误，所有操作均执行成功
            return
        fail_op_results = filter(self.__is_es_op_fail, bulk_result['items'])
        # 目前对失败的操作只是记录日志，后续可以考虑Redo
        app_log.error('ES bulk operation has errors:{0}', fail_op_results)
        return fail_op_results

    def get_es_bulk_result(self, bulk_result):
        """
        处理ES bulk操作结果
        :param bulk_result:
         {u'items':
            [{u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }},
            {u'update': {u'status': 404, u'_type': u'QmShopProduct', u'_id': u'20', u'error':
                          u'DocumentMissingException[[seach_test][-1] [QmShopProduct][20]: document missing]',
                          u'_index': u'seach_test'}},
            {u'index': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'301', u'_version': 2,
                        u'_index': u'seach_test'}}
            ],
        u'errors': True, u'took': 6}
        :return:
        """
        if not bulk_result or not bulk_result.get('errors'):
            return None
        op_results = map(lambda result_item: not self.__is_es_op_fail(result_item), bulk_result['items'])
        raise EsBulkOperationError(op_results)

    def __is_es_op_fail(self, op_result):
        """
        判断ES操作是否成功
        :param op_result: {u'update': {u'status': 200, u'_type': u'QmShopProduct', u'_id': u'1', u'_version': 15,
                          u'_index': u'seach_test' }}
        :return:
        """
        if not op_result:
            return True
        for key in op_result:
            return False if 'status' in op_result[key] and 200 <= op_result[key]['status'] < 300 else True


es_adapter = EsIndexAdapter()
