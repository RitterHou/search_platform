# -*- coding: utf-8 -*-
import time

from common.exceptions import MsgHandlingFailError

__author__ = 'liuzhaoming'

import httplib
import urllib
import ujson as json

from common.utils import get_dict_value_by_path, bind_variable, bind_dict_variable, COMBINE_SIGN, local_host_name, \
    format_time
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
        # data = [data]
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
        host = source_config['host'].format(**config.get_value('consts/custom_variables'))
        http_method = get_dict_value_by_path('request/http_method', source_config, 'POST')
        body_template = get_dict_value_by_path('request/body', source_config, {})
        body = bind_dict_variable(body_template, _request_param)
        timeout = get_dict_value_by_path('request/timeout', source_config, 60)
        if 'version' in body_template:
            body['version'] = version

        response = json.loads(self.call_http_method(host, get_method_url, http_method, body, timeout))
        # 如果返回code，抛出异常
        if 'code' in response:
            app_log.error(
                'Can not pull, source_config is {0}, version is {1}, request_param is {2}, response is {3} '
                'error code is {4}',
                source_config, version, _request_param, response, response.get('code'))
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
        app_log.info(
            'Call http method start by param : host = {0}, url={1}, method_name={2}, app_params={3}, timeout={4}, '
            'sys_params={5}, headers={6}',
            host, url, method_name, app_params, timeout, sys_params, headers)
        app_params.update(sys_params)
        start_time = time.time()
        result = None
        try:
            # h1 = httplib.HTTPConnection(host, timeout=timeout)
            h1 = httplib.HTTPConnection(host)
            h1.request("POST", url, urllib.urlencode(app_params), headers)
            response = h1.getresponse()
            result = response.read()
            app_log.info('Call http method finish successfully host = {0}, url={1}', host, url)

            cost_time = int((time.time() - start_time) * 1000)
            json_log_record = {'cost_time': cost_time, 'sender_host': local_host_name, 'sender_name': 'search_platform',
                               'receiver_host': host,
                               'invoke_time': format_time(start_time), 'message': 'Call http method is invoked',
                               'param_types': ['host', 'url', 'app_params'],
                               'param_values': [host, url, app_params],
                               'result_value': result}
            interface_log.print_log(json_log_record)
            return result
        except Exception as e:
            app_log.error('Call http method error host = {0}, url={1}', host, url)
            cost_time = int((time.time() - start_time) * 1000)
            json_log_record = {'cost_time': cost_time, 'sender_host': local_host_name, 'sender_name': 'search_platform',
                               'receiver_host': host,
                               'invoke_time': format_time(start_time), 'message': 'Call http method has error, {0}',
                               'param_types': ['host', 'url', 'app_params'],
                               'param_values': [host, url, app_params],
                               'result_value': result}
            interface_log.print_error(json_log_record, error=e)
            raise MsgHandlingFailError(MsgHandlingFailError.HTTP_ERROR)


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
        host = source_config['host'].format(**config.get_value('consts/custom_variables'))
        method = get_dict_value_by_path('request/method', source_config)
        body_template = get_dict_value_by_path('request/body', source_config, {})
        body = bind_dict_variable(body_template, _request_param)
        timeout = get_dict_value_by_path('request/timeout', source_config, 10)
        if 'version' in body_template:
            body['version'] = version

        response = self.call_dubbo_method(host, service_interface, method, body, version, timeout)
        # 如果返回code，抛出异常
        if response is None:
            app_log.error(
                'can not pull, service_interface is {0}, version is {1}, request_param is {2}, response is {3}',
                service_interface, version, body, response)
            return None

        return response['root'] if 'root' in response else response

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
        app_log.info(
            'Call JsonRPC method start by param : host = {0}, service_interface={1}, method={2}, body={3}, '
            'version={4}, timeout={5}', host, service_interface, method, body, version, timeout)
        start_time = time.time()
        result = None
        # reflect_method = getattr(dubbo_client, method)
        try:
            dubbo_client = DubboRegistryFactory.get_dubbo_client(host, service_interface, body, version, True)
            if body:
                result = dubbo_client.call_method(method, timeout, body)
            else:
                result = dubbo_client.call_method(method, timeout)
            app_log.info(
                'Call JsonRPC method finished successfully host = {0}, service_interface={1}, method={2}, body={3}',
                host, service_interface, method, body)

            cost_time = int((time.time() - start_time) * 1000)
            json_log_record = {'cost_time': cost_time, 'sender_host': local_host_name, 'sender_name': 'search_platform',
                               'receiver_host': host,
                               'invoke_time': format_time(start_time), 'message': 'Call JsonRPC method is invoked',
                               'param_types': ['host', 'service_interface', 'method', 'version', 'body'],
                               'param_values': [host, service_interface, method, version, body],
                               'srv_group': '{0}.{1}'.format(service_interface, method),
                               'result_value': result}
            interface_log.print_log(json_log_record)
            return result
        except Exception as e:
            app_log.error('Call JsonRPC method has error, host = {0}, service_interface={1}, method={2}, body={3}',
                          e, host, service_interface, method, body)
            cost_time = int((time.time() - start_time) * 1000)
            json_log_record = {'cost_time': cost_time, 'sender_host': local_host_name, 'sender_name': 'search_platform',
                               'receiver_host': host,
                               'invoke_time': format_time(start_time), 'message': 'Call JsonRPC method has error {0}',
                               'param_types': ['host', 'service_interface', 'method', 'version', 'body'],
                               'param_values': [host, service_interface, method, version, body],
                               'srv_group': '{0}.{1}'.format(service_interface, method),
                               'result_value': result}
            interface_log.print_error(json_log_record, e)
            raise MsgHandlingFailError(MsgHandlingFailError.DUBBO_ERROR)


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
