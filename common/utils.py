# -*- coding: utf-8 -*-
import inspect
import time
from datetime import datetime
import socket
from re import search
import os

from django.http import QueryDict



__author__ = 'liuzhaoming'


def singleton(cls, *args, **kw):
    """
    用装饰器实现单例
    :param cls:
    :param args:
    :param kw:
    :return:
    """
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


def get_dict_value(dict_input, key, default_value='', min_value='', max_value='', transform_fun=None):
    def _larger_than(num_1, num_2):
        """
        比较num1是否大于num2,如果不是数值类型的，返回false
        """
        if (isinstance(num_1, int) or isinstance(num_1, float)) and \
                (isinstance(num_2, int) or isinstance(num_2, float)):
            return num_1 > num_2
        return False

    def _less_than(num_1, num_2):
        """
        比较num1是否小于num2,如果不是数值类型的，返回false
        """
        if (isinstance(num_1, int) or isinstance(num_1, float)) and \
                (isinstance(num_2, int) or isinstance(num_2, float)):
            return num_1 < num_2
        return False

    value = dict_input[key] if key in dict_input else default_value
    if transform_fun:
        value = transform_fun(value)
    if max_value is not None and value is not None and _larger_than(value, max_value):
        value = default_value
    elif _less_than(value, min_value):
        value = default_value

    return value


def to_utf_chars(word):
    reg = repr(word.decode('utf8'))
    return reg[2:(len(reg) - 1)]


def bind_variable(expression, kwargs):
    """
    字符串中变量绑定,如果返回的变量为u'None', 则转换为None
    :param expression:
    :param kwargs:
    :return:
    """
    try:
        if isinstance(expression, str) or isinstance(expression, unicode):
            return expression.format(**kwargs)
    except:
        pass
    return expression


def bind_dict_variable(expr_dict, kwargs, is_create=True):
    """
    字典中value进行变量绑定
    :param expression:
    :param kwargs:
    :return:
    """
    if is_create:
        new_dict = {}
        for key in expr_dict:
            new_dict[key] = bind_variable(expr_dict[key], kwargs)
        return new_dict
    else:
        for key in expr_dict:
            expr_dict[key] = bind_variable(expr_dict[key], kwargs)
        return expr_dict


def unbind_variable(regex, variable_name, text):
    """
    字符串中变量解绑定
    :param regex:
    :param variable_name:
    :param text:
    :return:
    """
    m = search(regex, text)
    if not m:
        return variable_name, None
    variable_value = m.group(variable_name)
    return (variable_name, variable_value) if variable_value else (variable_name, None)


def get_dict_value_by_path(path, data_dict, default_value=None):
    """
    从字典中根据路径获取值，'/fethcer/kk/www'
    :param data_dict:
    :param path:
    :return:
    """

    def __get_value_from_dict(input_dict, key_list):
        """
        递归从字典中获取值
        :param key_list:
        :return:
        """
        if not key_list or not input_dict:
            return default_value
        elif len(key_list) == 1:
            return input_dict[key_list[0]] if key_list[0] in input_dict else default_value
        else:
            return __get_value_from_dict(input_dict[key_list[0]], key_list[1:]) \
                if key_list[0] in input_dict else default_value

    if not path:
        return default_value
    default_key_list = filter(lambda key: key, path.split('/'))
    return __get_value_from_dict(data_dict, default_key_list)


def merge(dict_param, other_dict_param):
    """
    合并字典，支持嵌套，返回一个全新字典
    :param dict_param:
    :param other_dict_param:
    :return:
    """
    result_dict = dict(dict_param)
    for key in other_dict_param:
        if isinstance(other_dict_param[key], dict) and key in dict_param and isinstance(dict_param[key], dict):
            result_dict[key] = merge(dict_param[key], other_dict_param[key])
        else:
            result_dict[key] = other_dict_param[key]
    return result_dict


def deep_merge(dict_param, other_dict_param):
    """
    合并字典，支持嵌套，如果属性是list iteratorable set，会进行两个list的合并
    :param dict_param:
    :param other_dict_param:
    :return:
    """
    if not dict_param or not other_dict_param:
        return dict_param or other_dict_param
    result_dict = dict(dict_param)
    for key in other_dict_param:
        if isinstance(other_dict_param[key], dict) and key in dict_param and isinstance(dict_param[key], dict):
            result_dict[key] = deep_merge(dict_param[key], other_dict_param[key])
        elif (isinstance(other_dict_param[key], list) or isinstance(other_dict_param[key], tuple)) and key in dict_param \
                and (isinstance(dict_param[key], list) or isinstance(dict_param[key], tuple)):
            result_dict[key] = result_dict[key] + other_dict_param[key]
        else:
            result_dict[key] = other_dict_param[key]
    return result_dict


def query_dict_to_normal_dict(query_dict):
    """
    将django QueryDict转化为普通的dict
    :param query_dict:
    :return:
    """
    if not query_dict:
        return {}
    if not isinstance(query_dict, QueryDict):
        return query_dict
    normal_dict = {}
    for (key, value_list) in query_dict.iteritems():
        if isinstance(value_list, (list, tuple)) and len(value_list) > 0:
            normal_dict[key] = value_list[0]
        else:
            normal_dict[key] = value_list
    return normal_dict


COMBINE_SIGN = '|||'


def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


def get_pid():
    """
    获取本进程ID
    :return:
    """
    return os.getpid()


def get_host_name():
    """
    获取机器名称
    :return:
    """
    return socket.gethostname()


local_host_name = get_host_name()


def get_client_id():
    """
    获取进程标识
    :return:
    """
    return '|||'.join((get_host_name(), str(get_pid())))


def get_function_params(fun_param):
    """
    获取函数的参数列表
    :param fun_param:
    :return:
    """
    if not fun_param:
        return ()
    return inspect.getargspec(fun_param)[0]


def get_default_es_host():
    """
    获取ES默认host
    :return:
    """
    from search_platform.settings import SERVICE_BASE_CONFIG

    return SERVICE_BASE_CONFIG['elasticsearch']


def get_time_by_mill():
    """
    获取当前毫秒时间
    :return:
    """
    return int(time.time() * 1000)


def get_time_by_mill_str(format="%Y-%m-%dT%H:%M:%S."):
    """
    获取毫秒时间，根据指定的格式
    :param format:
    :return:
    """
    today = datetime.utcnow()
    str_time = today.strftime(format)
    mills = today.microsecond / 1000
    return ''.join((str_time, str(mills), 'Z'))


def format_time(time_stamp, format='%Y-%m-%dT%H:%M:%S.%f'):
    """
    格式化时间
    :param time_stamp:
    :param format:
    :return:
    """
    date = datetime.fromtimestamp(time_stamp)
    return date.strftime(format)


def format_dict(_input_dict):
    """
    使用simple json格式化dict
    :param _input_dict:
    :return:
    """
    import ujson as json

    if not _input_dict:
        return '{}'

    try:
        if isinstance(_input_dict, QueryDict):
            _input_dict = _input_dict.lists()
        return json.dumps(_input_dict)
    except:
        return str(_input_dict)


if __name__ == '__main__':
    # print to_utf_chars('China u中华人民共和国 ￥$end')
    print 'bind_variable test start........'
    print bind_variable('/products/{adminId}/{version}/{ids}',
                        {'adminId': 'a100000', 'version': '1.2', 'ids': ['11', '34', '9876'], 'test': 'kkkkk'})
    print 'unbind_variable test start........'
    print unbind_variable('\|(?P<adminId>a\\d+)\|', 'adminId', 'pwtew|aest222222|a0123456789|kkkerwerw|4*&(&4')
    str_temp = 'abd123kg'
    m = search('444', str_temp)
    m = search("msgtype=(?P<msg_type>444)", str_temp)

    dict1 = {'dict1_key1': ["value1"], 'dict1_key2': "value2", 'dict1_key3': {'1': 'dic1_1', '2': '2222'}, }
    dict2 = {'dict1_key1': "value1+2", 'dict1_key2': ["value2+2"], 'dict1_key3': {'12': 'dic2_1+2', '2': 'dic2_2+2'}, }
    print merge(dict1, dict2)

    print get_function_params(merge)
