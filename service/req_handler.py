# -*- coding: utf-8 -*-
from rest_framework import status
from rest_framework.response import Response

from common.data_parsers import item_parser
from service.req_filter import request_filter
from service import (get_request_data, desc_request, get_url)
from service.models import *
from common.loggers import query_log as app_log
from common.utils import merge, query_dict_to_normal_dict
from common.configs import config


__author__ = 'liuzhaoming'


class RequestHandler(object):
    """
    HTTP请求处理器
    """
    RES_TYPE_DICT = {'product': Product, 'aggregation': Aggregation, 'suggest': Suggest, 'search': Search,
                     'measure': Stats, 'ex_measure': ExStats, 'ex_suggest': ExSuggest, 'recommendation': Recommendation,
                     'default': SearchPlatformDoc}

    def __init__(self, handler_config):
        self.handler_config = handler_config
        self.filter_config = handler_config.get('filter')

    def handle(self, request, format):
        """
        处理HTTP请求
        :param request:
        :param format:
        :return:
        """
        try:
            app_log.info("Receive http request : {0}", desc_request(request))
            destination_config = self.handler_config.get('destination')
            if not destination_config:
                # app_log.error('The destination_config is invalid, {0}'.format(self.handler_config))
                destination_config = {}

            if request.method not in self.__get_support_http_methods():
                return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)
            if request.method == 'GET':
                return self.__fetch_from_es(request, destination_config, format)
            elif request.method == 'POST':
                return self.post(request, destination_config)
            elif request.method == 'DELETE':
                return self.delete(request, destination_config)
            elif request.method == 'PUT':
                return self.put(request, destination_config)
        except Exception as e:
            app_log.error('The destination_config is invalid, {0}, {1}'.format(self.handler_config, request))
            app_log.exception(e)
            raise e

    def match(self, request):
        """
        是否匹配请求
        :param request:
        :return:
        """
        return request_filter.filter(request, self.filter_config)

    def __get_es_config(self, destination_config, field_values):
        """
        获取ES配置参数
        :param destination_config:
        :return:
        """
        if 'reference' in destination_config:
            es_config = config.get_value('es_index_setting/' + destination_config['reference'])
            es_config = merge(es_config, destination_config)
            assert es_config, 'the reference is not exist, reference={0}'.format(destination_config)
        else:
            es_config = dict(destination_config)
        index, doc_type, doc_id = es_adapter.get_es_doc_keys(es_config, kwargs=field_values)
        es_config['index'] = index
        es_config['type'] = doc_type
        es_config['id'] = doc_id
        return es_config

    def __fetch_from_es(self, request, destination_config, format):
        """
        从ES获取数据
        :param request:
        :param destination_config:
        :param format:
        :return:
        """
        start_time = time.time()
        field_values = self.__get_fields_value(request)
        es_config = self.__get_es_config(destination_config, field_values)

        res_type = self.handler_config.get('res_type', 'product')
        model = self.__get_model(res_type)
        if model:
            result = model.objects.get(es_config, index_name=es_config['index'], doc_type=es_config['type'],
                                       args=get_request_data(request), parse_fields=field_values)
            print '__fetch_from_es spends {0}'.format(time.time() - start_time)
            return result

    def post(self, request, destination_config):
        """
        执行POST操作
        :param request:
        :param destination_config:
        :return:
        """
        field_values = self.__get_fields_value(request)
        es_config = self.__get_es_config(destination_config, field_values)

        res_type = self.handler_config.get('res_type', 'product')
        model = self.__get_model(res_type)
        if model:
            return model.objects.save(es_config, index_name=es_config['index'], doc_type=es_config['type'],
                                      product=get_request_data(request), parse_fields=field_values)

    def put(self, request, destination_config):
        """
        执行put操作
        :param request:
        :param destination_config:
        :return:
        """
        field_values = self.__get_fields_value(request)
        es_config = self.__get_es_config(destination_config, field_values, parse_fields=field_values)

        res_type = self.handler_config.get('res_type', 'product')
        model = self.__get_model(res_type)
        if model:
            return model.objects.update(es_config, index_name=es_config['index'], doc_type=es_config['type'],
                                        product=get_request_data(request))

    def delete(self, request, destination_config):
        """
        执行delete操作
        :param request:
        :param destination_config:
        :return:
        """
        field_values = self.__get_fields_value(request)
        es_config = self.__get_es_config(destination_config, field_values)

        res_type = self.handler_config.get('res_type', 'product')
        model = self.__get_model(res_type)
        if model:
            product = query_dict_to_normal_dict(get_request_data(request))
            return model.objects.delete(es_config, index_name=es_config['index'], doc_type=es_config['type'],
                                        product=product, parse_fields=field_values)

    def __get_model(self, res_type):
        """
        获取Model
        :param res_type:
        :return:
        """
        res = self.RES_TYPE_DICT.get(res_type)
        return SearchPlatformDoc if not res else res

    def __get_fields_value(self, request):
        """
        组合出提供给查询的值
        :param request:
        :return:
        """
        if 'data_parser' not in self.handler_config or not self.handler_config['data_parser']:
            app_log.error('The data_parser is invalid, {0}'.format(self.handler_config))
            return {}
        data_parser_config = self.handler_config['data_parser']

        if 'fields' not in data_parser_config or not data_parser_config['fields']:
            app_log.info('The data_parser fields is none')
            return {}

        data = {'url': get_url(request), 'param': get_request_data(request)}
        return self.__parse_field_items(data, data_parser_config)

    def __parse_field_items(self, data, data_parser_config):
        def __get_field_parser_input_data(item_config):
            """
            获取变量解析的输入数据，即该变量是从url中解析还是从请求参数中获取
            :param item_config:
            :return:
            """
            if not isinstance(item_config, dict) or 'input_data' not in item_config or item_config[
                'input_data'] == 'url':
                # 不配置默认从URL中解析
                return data['url']
            elif item_config['input_data'].startswith('param'):
                temp_strs = item_config['input_data'].split('.')
                return str(data['param'].get(temp_strs[1])) if len(temp_strs) == 2 else str(data['param'])
            return data['url']


        parser_type = data_parser_config.get('type', 'regex')
        fields_config = data_parser_config.get('fields')
        field_key_value_list = [
            item_parser.parse_item(__get_field_parser_input_data(item_config), item_config, field_name, parser_type) for
            field_name, item_config in fields_config.iteritems()]
        parse_result = dict(filter(lambda (key, value): value, field_key_value_list))
        return parse_result

    def __get_support_http_methods(self):
        """
        获取配置中支持的HTTP 操作，主要有：GET  POST  PUT  DELETE
        :return:
        """
        str_http_methods = self.handler_config.get('http_method').upper() or 'GET'
        return map(lambda http_method: http_method.strip(), str_http_methods.split(','))


if __name__ == '__main__':
    print "adminID=(?P<adminID>[\d\D]+?);".find('(?P<adminID>')
