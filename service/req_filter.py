# -*- coding: utf-8 -*-
from re import search

from service import get_url, get_request_data


__author__ = 'liuzhaoming'


class RequestFilter(object):
    """
    请求过滤器
    """

    def filter(self, request, filter_config):
        """
        判断是否符合所有匹配条件
        """
        if 'conditions' not in filter_config or not filter_config['conditions']:
            return True
        conditions = filter_config['conditions']
        union_operator = filter_config.get('union_operator', 'and')
        for condition in conditions:
            match_result = self.__match_single_condition(request, condition)
            if union_operator == 'and' and not match_result:
                return False
            elif union_operator == 'or' and match_result:
                return True
        return True if union_operator == 'and' else False

    def __match_single_condition(self, request, condition):
        """
        判断是否符合单个匹配条件
        """
        operator = condition['operator']
        match_result = False
        if condition.get('type') == 'regex':
            # 正则表达式匹配条件
            match_field = condition.get('field', 'url')
            if 'url' == match_field:
                # URL匹配
                match_text = get_url(request)
                match_result = True if search(condition['expression'], match_text) else False
            elif 'param' == match_field:
                # HTTP Request 附加参数匹配
                match_result = self.__match_request_param(request, condition)

        match_result = not match_result if operator == 'not' else match_result
        return match_result

    def __match_request_param(self, request, condition):
        """
        判断HTTP附加请求参数是否匹配条件
        :param request:
        :param condition:
        :return:
        """
        request_param = request.QUERY_PARAMS
        contains_cfg_items = condition.get('contains')
        match_result = True
        if not contains_cfg_items:
            return match_result
        for contains_cfg_item in contains_cfg_items:
            cfg_name = contains_cfg_item.get('name')
            cfg_value = contains_cfg_item.get('value')
            if not cfg_name:
                continue
            actual_value = request_param.get(cfg_name)
            if cfg_value != actual_value:
                match_result = False
                break
        return match_result


request_filter = RequestFilter()