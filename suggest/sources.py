# coding=utf-8
from collections import OrderedDict
from itertools import chain

import ujson as json
from common.configs import config
from common.connections import EsConnectionFactory
from common.es_routers import es_router
from common.utils import get_dict_value_by_path, unbind_variable
from common.adapter import es_adapter
from common.loggers import app_log
from service.dsl_parser import qdsl_parser

__author__ = 'liuzhaoming'


class SuggestSource(object):
    def pull(self, suggest_config, request_param, suggest_term_dict={}):
        """
        从数据源拉出数据
        :param suggest_config:
        :param request_param:
        :param suggest_term_dict
        :return:
        """
        app_log.info('Pull is called, suggest_config={0} , request_param={1}',
                     suggest_config['name'] if 'name' in suggest_config else '', request_param)
        source_key = self.__get_source_key(suggest_config)
        _data_source = DATA_SOURCE_DICT.get(source_key)
        if not _data_source:
            app_log.warning("cannot find data source with source_config = {0}", suggest_config)
        data = _data_source.pull(suggest_config, request_param, suggest_term_dict)
        # if not isinstance(data, list) and not isinstance(data, tuple) and data:
        # data = [data]
        return data

    @staticmethod
    def is_iterable(suggest_config):
        source_type = get_dict_value_by_path('source/type', suggest_config, 'get')
        return source_type.startswith('iterator')

    def __get_source_key(self, suggest_config):
        return get_dict_value_by_path('source/type', suggest_config, 'iterator_es_get')


class ElasticsearchDataSource(SuggestSource):
    def pull(self, suggest_config, request_param, suggest_term_dict):
        if 'index' not in request_param or 'type' not in request_param:
            app_log.error('request_param is invalid, {0} {1}', suggest_config, request_param)
            return 0, None

        host = get_dict_value_by_path('notification/host', suggest_config) or get_dict_value_by_path(
            'notification/es_cfg/host', suggest_config)
        source_config = get_dict_value_by_path('source', suggest_config)
        additional_param = self._parse_param(source_config, request_param)
        query_fields = self.__get_es_query_fields(source_config)
        source_docs = self.__query_source_docs(source_config, request_param, host, query_fields)
        if source_docs['total'] == 0:
            return source_docs

        field_mapping_result = es_adapter.get_fields_mapping(host, request_param['index'], request_param['type'],
                                                             query_fields)
        field_mapping_dict = field_mapping_result[request_param['index']]['mappings'][request_param['type']]
        keyword_filter_regex = str(get_dict_value_by_path('data_parser/keyword_filter_regex',
                                                          source_config) or u'[\u4e00-\u9fa5A-Za-z0-9]+')
        temp_list_list = [
            self.__get_keywords_from_doc(host, keyword_filter_regex, query_fields, source_doc, field_mapping_dict,
                                         request_param) for source_doc in source_docs['root']]
        keyword_list = list(chain(*temp_list_list))
        for _keyword in keyword_list:
            if suggest_term_dict.get(_keyword):
                suggest_term_dict[_keyword] += 1
            else:
                suggest_term_dict[_keyword] = 1
        keyword_list = set(keyword_list)

        # count_dsl_list = [({"search_type": "count"}, self._get_count_query_dsl(keyword, host, suggest_config).values())
        # for keyword in keyword_list]
        # count_body = chain(*count_dsl_list)
        # es_count_result_list = es_adapter.multi_search(count_body, host, request_param['index'], request_param['type'])
        source_type_weight = config.get_value('consts/suggest/source_type/1')
        # keyword_hits = map(lambda count_result: count_result['hits']['total'], es_count_result_list['responses'])
        # keyword_hits = self._get_keyword_hits_list(keyword_list, request_param, host, suggest_config)
        term_list = [
            dict({'word': keyword, 'source_type': '1', 'source_type_weight': source_type_weight},
                 **additional_param) for keyword in keyword_list]
        source_docs['root'] = term_list
        return source_docs

    def _get_keyword_hits_list(self, keyword_list, request_param, host, suggest_config):
        """
        整理各个关键词的
        :param keyword_list:
        :param request_param:
        :param host:
        :param suggest_config:
        :return:
        """
        count_dsl_list = chain(
            *[self._get_count_query_dsl(keyword, host, suggest_config).values() for keyword in keyword_list])
        count_dsl_list = map(lambda x: ({"search_type": "count"}, x), count_dsl_list)
        count_body = chain(*count_dsl_list)
        es_count_result_list = es_adapter.multi_search(count_body, host, request_param['index'], request_param['type'])
        keyword_hits = map(lambda count_result: count_result['hits']['total'], es_count_result_list['responses'])
        suggest_tags_cfg = get_dict_value_by_path('/source/tags', suggest_config)

        tag_name_list = list(suggest_tags_cfg.iterkeys())
        tags_length = len(tag_name_list) if suggest_tags_cfg else 1

        tags_hits_list = []
        count = 0
        for hit_num in keyword_hits:
            if count == 0:
                tag_hits_data = {}
            tag_hits_data[tag_name_list[count]] = hit_num
            if count == tags_length - 1:
                count = 0
                tags_hits_list.append(tag_hits_data)
            else:
                count += 1

        return tags_hits_list

    def __get_keywords_from_doc(self, host, keyword_filter_regex, query_fields, source_doc, field_mapping_dict,
                                request_param):
        """
        根据field mapping中指定的分词器对关键词进行分词
        :param host:
        :param keyword_filter_regex:
        :param query_fields:
        :param source_doc:
        :param field_mapping_dict:
        :return:
        """
        keyword_list = []
        for field_name in query_fields:
            if not source_doc.get(field_name):
                continue
            text = source_doc.get(field_name)
            if field_name in field_mapping_dict:
                field_mapping = get_dict_value_by_path(field_name + '/mapping/' + field_name, field_mapping_dict)
                field_analyzer = field_mapping.get('analyzer') or field_mapping.get('index_analyzer')
            if field_analyzer:
                analyzed_words = es_adapter.query_text_analyze_result(host, field_analyzer, text,
                                                                      keyword_filter_regex, request_param)
                keyword_list.extend(analyzed_words)
            else:
                keyword_list.append(text)
        return keyword_list

    def __query_source_docs(self, source_config, request_param, host, query_fields):
        try:
            query_body = self.__get_es_query_body(source_config, request_param)
            # query_param = {'fields': query_fields} if query_fields else {}
            es_result = es_adapter.query_docs(query_body, host, index=request_param['index'],
                                              doc_type=request_param['type'])

        except Exception as e:
            app_log.error('pull has exception with {0} {1}', e, source_config, request_param)
            es_result = {}
        return self.__parse_es_result(es_result)

    def __get_es_query_body(self, source_config, request_param):
        """
        获取ES查询QDSL
        :param source_config:
        :param request_param:
        :return:
        """
        size = source_config['size'] if 'size' in source_config else config.get_value(
            'consts/suggest/default_es_iterator_get_size')
        pos_from = request_param['from'] if 'from' in request_param else 0
        if request_param.get('hashcode'):
            return {'query': {'term': {"_adminId": request_param['adminId']}}, 'size': size, 'from': pos_from}
        else:
            return {'query': {'match_all': {}}, 'size': size, 'from': pos_from}

    def __get_es_query_fields(self, source_config):
        """
        获取查询字段
        :param source_config:
        :return:
        """
        fields_dict = get_dict_value_by_path('data_parser/fields', source_config)
        return list(fields_dict.itervalues()) if fields_dict else []

    def __parse_es_result(self, es_result):
        """
        解析ES查询结果
        :param es_result:
        :return:
        """
        if 'hits' in es_result and es_result['hits'] and 'hits' in es_result['hits']:
            total = es_result['hits']['total']
            doc_list = es_result['hits']['hits']
            product_list = map(lambda doc: doc['_source'], doc_list)
        elif '_source' in es_result:
            total = 1
            product_list = [es_result['_source']]
        else:
            total = 0
            product_list = []

        return {'root': product_list, 'total': total, 'curSize': len(product_list)}

    def _parse_param(self, source_config, request_param):
        """
        从给定的source参数中解析出需要的变量，目前主要是从索引或type中解析出adminId
        :param source_config:
        :param request_param:
        :return:
        """
        result = {}
        param_parser_config = source_config.get('param_parser')
        if not param_parser_config or not param_parser_config.get('fields'):
            return result

        fields_config_dict = param_parser_config.get('fields')
        for field_name in fields_config_dict:
            field_parser_config = fields_config_dict[field_name]
            parser_type = field_parser_config.get('type', 'regex')
            if parser_type == 'regex':
                name, field_value = unbind_variable(field_parser_config['expression'], field_name,
                                                    request_param[field_parser_config['field']])
                result[field_name] = field_value
        if 'adminId' not in result or not result['adminId']:
            result['adminId'] = request_param['adminId']
        return result

    def _get_count_query_dsl(self, keyword, host, suggest_config=None):
        """
        和搜索同步修改算法，现在改为对关键词进行标准分词，对分词后的结果进行_all字段查询
        2015.09.15修改，改为调用query DSL解析方法，并增加多个tag
        :param keyword:
        :param host:
        :return:
        """

        def get_extended_dsl(_extended_dsl):
            """
            将配置文件中的DSL转换为must查询中得DSL
            """
            if isinstance(_extended_dsl, (str, unicode)):
                _extended_dsl = json.loads(_extended_dsl)
                return [_extended_dsl]
            elif isinstance(_extended_dsl, dict):
                return [_extended_dsl]
            elif isinstance(_extended_dsl, (list, tuple, set)):
                return _extended_dsl
            return []

        es_connection = EsConnectionFactory.get_es_connection(host=host)
        must_body = qdsl_parser.parse_query_string_condition(keyword, es_connection, {})
        # analyze_token_list = es_adapter.query_text_analyze_result_without_filter(
        # es_connection=None, analyzer='standard', host=host, text=keyword)
        # if analyze_token_list:
        # must_body = map(lambda analyze_token: {'match': {'_all': analyze_token}}, analyze_token_list)
        # else:
        # must_body = [{'match': {'_all': keyword}}]
        count_query_dsl_dict = OrderedDict()
        if suggest_config:
            # extended_dsl = get_dict_value_by_path('/source/extended_dsl', suggest_config)
            # count_query_dsl_dict['default'] = {
            # 'query': {'bool': {'must': must_body + get_extended_dsl(extended_dsl)}}}

            # 增加tags
            suggest_tags_cfg = get_dict_value_by_path('/source/tags', suggest_config)
            if suggest_tags_cfg:
                for (tag_name, tag_dsl) in suggest_tags_cfg.iteritems():
                    if tag_name:
                        count_query_dsl_dict[tag_name] = {
                            'query': {'bool': {'must': must_body + get_extended_dsl(tag_dsl)}}}
        else:
            count_query_dsl_dict['default'] = {'query': {'bool': {'must': must_body}}}
        return count_query_dsl_dict


class SpecifyWordsDataSource(ElasticsearchDataSource):
    """
    添加指定词汇的Suggest
    """

    def pull(self, suggest_config, request_param, suggest_term_dict={}):
        es_cfg = es_router.merge_es_config(get_dict_value_by_path('/notification/es_cfg', suggest_config))
        request_param['index'], request_param['type'], _id = es_router.get_es_doc_keys(es_cfg, request_param)

        host = es_cfg['host']
        additional_param = {'adminId': request_param['adminId']} if 'adminId' in request_param else {}

        if isinstance(request_param['word'], (set, tuple, list)):
            keyword_list = request_param['word']
        else:
            keyword_list = [request_param['word']]
        source_docs = {'total': len(keyword_list)}
        # *[self._get_count_query_dsl(keyword, host, suggest_config).values() for keyword in keyword_list])
        # count_dsl_list = map(lambda x: ({"search_type": "count"}, x), count_dsl_list)
        # count_body = list(chain(*count_dsl_list))
        # es_count_result_list = es_adapter.multi_search(count_body, host, index, doc_type)
        source_type_weight = config.get_value('consts/suggest/source_type/' + request_param['source_type'])
        keyword_hits = self._get_keyword_hits_list(keyword_list, request_param, host, suggest_config)
        term_list = [
            dict({'word': keyword, 'hits': keyword_hits, 'source_type': request_param['source_type'],
                  'source_type_weight': source_type_weight},
                 **additional_param) for (keyword, keyword_hits) in zip(keyword_list, keyword_hits) if keyword_hits > 0]
        source_docs['root'] = filter(lambda _term: _term['hits']['default'], term_list)

        return source_docs


DATA_SOURCE_DICT = {'iterator_es_get': ElasticsearchDataSource(), 'specify_words': SpecifyWordsDataSource()}

suggest_source = SuggestSource()

if __name__ == '__main__':
    query_body = {'query': {'match_all': {}}, 'size': 10, 'from': 0}
    es_result = es_adapter.query_docs(query_body=query_body, host='http://172.19.65.66:9200/', index='qmshop-test',
                                      doc_type='QmShopProduct',
                                      params={})
    print es_result
