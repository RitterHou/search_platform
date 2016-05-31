# coding=utf-8
"""
REST request对象无法序列化，需要封装一个简单类型的实现类，便于序列化
"""
from collections import namedtuple

__author__ = 'liuzhaoming'

WrapperRequest = namedtuple('WrapperRequest', ['path'])


class RestRequest(object):
    """
    rest request对象
    """

    def __init__(self, origin_request=None):
        if origin_request:
            self.full_path = origin_request.get_full_path()
            self.QUERY_PARAMS = origin_request.QUERY_PARAMS
            self.DATA = origin_request.DATA
            self.method = origin_request.method
            self._request = WrapperRequest(origin_request._request.path)
            self.META = {}
            for (key, value) in origin_request.META.iteritems():
                if isinstance(value, str):
                    self.META[key] = value

    def get_full_path(self):
        return self.full_path