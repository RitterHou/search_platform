# -*- coding: utf-8 -*-
from collections import OrderedDict
import json
import time

import re


__author__ = 'liuzhaoming'

from itertools import *

from elasticsearch import ElasticsearchException

from common.utils import get_dict_value_by_path, bind_variable
from common.loggers import app_log, debug_log
from common.configs import config
from common.connections import EsConnectionFactory
from search_platform.settings import SERVICE_BASE_CONFIG


class EsIndexAdapter(object):
    """
    ES索引数据适配器
    """

    def batch_create(self, es_config, doc_list):
        """
        批量创建或者更新文档，如果是更新文档，需要带上所有属性，
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc_list[0])
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_create_body(es_config, doc_list=doc_list)
        try:
            es_start_time = time.time()
            es_bulk_result = es_connection.bulk(bulk_body)
            app_log.info('es spend time {0}'.format(time.time() - es_start_time))
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))


    def batch_update(self, es_config, doc_list):
        """
        批量更新文档，只需要给定要更新的字段即可
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc_list[0])
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_update_body(es_config, doc_list=doc_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body)
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))


    def batch_delete(self, es_config, request_param):
        """
        批量删除文档
        :param es_config:
        :param request_param:
        :return:
        """
        if not request_param:
            return
        if isinstance(request_param, tuple) or isinstance(request_param, list):
            request_param = request_param[0]

        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=request_param)
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
            es_bulk_result = es_connection.bulk(bulk_body)
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('ES operation input param is {0}', e, list(bulk_body))

    def batch_delete_by_ids(self, es_config, doc_ids, message_parse_result={}, separator=':'):
        if not doc_ids:
            return
        doc_id_list = doc_ids.strip().strip(';').split(separator)
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=message_parse_result)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        bulk_body = self.__build_batch_delete_body_by_ids(es_config, index, doc_type, doc_id_list)
        try:
            es_bulk_result = es_connection.bulk(bulk_body)
            return self.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('es operation input param is {0}', e, list(bulk_body))


    def delete_all_doc(self, es_config, doc):
        """
        删除type中得所有文档
        :param es_config:
        :return:
        """
        index, doc_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=doc_type, version=config.get_value('version')))
        try:
            es_connection.delete_by_query(index=index, doc_type=doc_type, body={'query': {'match_all': {}}})
        except ElasticsearchException as e:
            app_log.error('es operation input param is {0}, {1}', e, index, doc_type)


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
        return bind_variable(index_template, params).lower(), bind_variable(type_template, params), \
               doc_id if doc_id else bind_variable(id_template, params)

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

    def query_text_analyze_result_without_filter(self, es_connection, analyzer, text, host=None):
        """
        对文本进行分词,不支持过滤
        :param es_connection:
        :param analyzer:
        :param text:
        :return:
        """
        if not es_connection:
            es_connection = EsConnectionFactory.get_es_connection(host=host)
        analyze_result = es_connection.indices.analyze(params={'analyzer': analyzer, 'text': text})
        return list(set([ele['token'] for ele in analyze_result['tokens'] if len(ele['token']) > 0]))


    def multi_search(self, body, host, index, doc_type):
        """
        批量搜索
        :param body:
        :param host:
        :param index:
        :param doc_type:
        :return:
        """
        es_connection = EsConnectionFactory.get_es_connection(host=host)
        return es_connection.msearch(body, index, doc_type)

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
                update_body.append({'doc': {'json_str': json.dumps(default_config_data[key])}})
        try:
            es_bulk_result = es_connection.bulk(update_body)
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
                update_body.append({'json_str': json.dumps(default_config_data[key])})
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


    def __build_batch_create_body(self, es_config, doc_list):
        """
        构造商品列表批量更新的ES数据结构，,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return []
        return chain(*map(lambda doc: self.__build_doc_create_body(es_config, doc=doc), doc_list))

    def __build_batch_update_body(self, es_config, doc_list):
        """
        构造商品列表批量更新的ES数据结构，只需要给定要更新的字段即可,此处可以优化，不需要每次都获取index和type
        :param es_config:
        :param doc_list:
        :return:
        """
        if not doc_list:
            return []
        return chain(*map(lambda doc: self.__build_doc_update_body(es_config, doc=doc), doc_list))

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

    def __build_doc_create_body(self, es_config, doc):
        """
        构建单个文档的批量创建数据结构，如果文档已经存在，则为更新操作,无论文档是否存在，都需要提供文档全量数据
        :param es_config:
        :param doc:
        :return:
        """
        if not doc:
            return ()
        index, es_type, doc_id = self.get_es_doc_keys(es_config, kwargs=doc)
        return {"index": {"_index": index, "_type": es_type, "_id": doc_id}}, doc

    def __build_doc_update_body(self, es_config, doc_id=None, doc=None):
        """
        构建单个文档批量更新数据结构,文本必须存在，不需要提供全量数据
        :param es_config:
        :param doc_id:
        :param doc:
        :return:
        """
        if not doc:
            return ()
        index, es_type, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"update": {"_index": index, "_type": es_type, "_id": doc_id}}, {"doc": doc}

    def __build_doc_delete_body(self, es_config, doc_id=None, doc=None):
        """
        构建单个文档批量删除数据结构
        :param es_config:
        :param doc_id:
        :param doc:
        :return:
        """
        index, es_type, doc_id = self.get_es_doc_keys(es_config, doc_id, kwargs=doc)
        return {"delete": {"_index": index, "_type": es_type, "_id": doc_id}}

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

