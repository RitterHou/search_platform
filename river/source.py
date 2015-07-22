# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

import httplib
import urllib
import json

from common.utils import get_dict_value_by_path, bind_variable, bind_dict_variable, COMBINE_SIGN
from common.configs import config
from common.loggers import debug_log, app_log, interface_log
from common.connections import DubboRegistryFactory


class DataSource(object):
    """
    数据源
    """

    @debug_log.debug('DataSource.pull')
    def pull(self, source_config, request_param):
        """
        从数据源拉出数据
        :param source_config:
        :param request_param:
        :return:
        """
        source_key = get_source_key(source_config)
        _data_source = DATA_SOURCE_DICT[source_key]
        if not _data_source:
            app_log.warning("Cannot find data source with source_config = {0}", source_config)
        data = _data_source.pull(source_config, request_param)
        # if not isinstance(data, list) and not isinstance(data, tuple) and data:
        #     data = [data]
        return data

    @staticmethod
    def is_iterable(source_config):
        source_type = get_dict_value_by_path('type', source_config, 'get')
        return source_type.startswith('iterator')


class HttpDataSource(DataSource):
    """
    HTTP数据源
    """

    @debug_log.debug('HttpDataSource.pull')
    def pull(self, source_config, request_param):
        """
        从数据源拉取数据
        :param source_config:
        :param _request_param:
        :return:
        """
        _request_param = request_param['fields']
        version = config.get_value('version')
        if 'version' not in _request_param:
            _request_param['version'] = version
        get_method_url_template = get_dict_value_by_path('request/url', source_config)
        get_method_url = bind_variable(get_method_url_template, _request_param)
        host = source_config['host']
        http_method = get_dict_value_by_path('request/http_method', source_config, 'POST')
        body_template = get_dict_value_by_path('request/body', source_config, {})
        body = bind_dict_variable(body_template, _request_param)
        timeout = get_dict_value_by_path('request/timeout', source_config, 60)
        if 'version' in body_template:
            body['version'] = version
        try:
            response = json.loads(self.call_http_method(host, get_method_url, http_method, body, timeout))
            # 如果返回code，抛出异常
            if 'code' in response:
                app_log.error(
                    'Can not pull, source_config is {0}, version is {1}, request_param is {2}, response is {3} '
                    'error code is {4}',
                    source_config, version, _request_param, response, response.get('code'))
                return None
        except Exception as e:
            app_log.exception(e)
            return None

        return response['root'] if 'root' in response else response

    @debug_log.debug('HttpDataSource._call_http_method')
    def call_http_method(self, host, url, method_name, app_params, timeout=60):
        """
        调用HTTP方法
        :param host:
        :param url:
        :param method_name:
        :param version:
        :param app_params:
        :return:
        """
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": "application/json"}
        sys_params = {
            'method': method_name,
            'format': 'json'
        }
        interface_log.print_log(
            'Call http method start by param : host = {0}, url={1}, method_name={2}, app_params={3}, timeout={4}, '
            'sys_params={5}, headers={6}',
            host, url, method_name, app_params, timeout, sys_params, headers)
        app_params.update(sys_params)
        h1 = httplib.HTTPConnection(host, timeout=timeout)
        h1.request("POST", url, urllib.urlencode(app_params), headers)
        response = h1.getresponse()
        result = response.read()
        interface_log.print_log('Call http method finish with result : {0}', result)
        return result


class PassthroughSource(DataSource):
    """
    直接透传
    """

    def pull(self, source_config, request_param):
        """
        直接透传数据
        :param source_config:
        :param request_param:
        :return:
        """
        return request_param[source_config['fields_reference']] if 'fields_reference' in source_config else \
            request_param['fields']


class DubboDataSource(DataSource):
    """
    Dubbo数据源
    """

    @debug_log.debug('DubboDataSource.pull')
    def pull(self, source_config, request_param):
        """
        从数据源拉取数据
        :param source_config:
        :param _request_param:
        :return:
        """
        _request_param = request_param['fields']
        version = get_dict_value_by_path('version', source_config) or config.get_value('version')
        if 'version' not in _request_param:
            _request_param['version'] = version
        service_interface_template = get_dict_value_by_path('service_interface', source_config)
        service_interface = bind_variable(service_interface_template, _request_param)
        host = source_config['host']
        method = get_dict_value_by_path('request/method', source_config)
        body_template = get_dict_value_by_path('request/body', source_config, {})
        body = bind_dict_variable(body_template, _request_param)
        timeout = get_dict_value_by_path('request/timeout', source_config, 60)
        if 'version' in body_template:
            body['version'] = version
        try:
            response = self.call_dubbo_method(host, service_interface, method, body, version, timeout)
            # 如果返回code，抛出异常
            if not response:
                app_log.error(
                    'can not pull, source_config is {0}, version is {1}, request_param is {2}, response is {3}', None,
                    source_config, version, _request_param, response)
                return None
        except Exception as e:
            app_log.exception(e)
            return None

        return response['root'] if 'root' in response else response

    @debug_log.debug('DubboDataSource.call_method')
    def call_dubbo_method(self, host, service_interface, method, body, version, timeout=120):
        """
        调用Dubbo方法
        :param host:
        :param url:
        :param method_name:
        :param version:
        :param app_params:
        :return:
        """
        interface_log.print_log(
            'Call JsonRPC method start by param : host = {0}, service_interface={1}, method={2}, body={3}, '
            'version={4}, timeout={5}',
            host, service_interface, method, body, version, timeout)
        dubbo_client = DubboRegistryFactory.get_dubbo_client(host, service_interface, body, version)
        # reflect_method = getattr(dubbo_client, method)
        if body:
            result = dubbo_client(method, body)
        else:
            result = dubbo_client(method)
        interface_log.print_log('Call JsonRPC method finish with result : {0}', result)
        return result


def get_source_key(source_config):
    source_type = get_dict_value_by_path('type', source_config, 'get')
    source_protocol = get_dict_value_by_path('protocol', source_config, 'http')
    return ''.join((source_type, COMBINE_SIGN, source_protocol))


DATA_SOURCE_DICT = {get_source_key({"type": "iterator_get", "protocol": "http"}): HttpDataSource(),
                    get_source_key({"type": "get", "protocol": "http"}): HttpDataSource(),
                    get_source_key({"type": "passthrough"}): PassthroughSource(),
                    get_source_key({"type": "get", "protocol": "dubbo"}): DubboDataSource(),
                    get_source_key({"type": "iterator_get", "protocol": "dubbo"}): DubboDataSource(), }

source = DataSource()