# coding=utf-8

from re import search

from common.loggers import app_log


__author__ = 'liuzhaoming'


class NotificationFilter(object):
    """
    Suggest消息触发器过滤器
    """

    def filter(self, notification, filter_config):
        """
        触发器过滤
        """
        if not filter_config:
            return True
        elif 'type' not in filter_config:
            app_log.error('filter config is invalid because has no type, {0}', filter_config)
            return False

        _filter = FILTERS_DICT.get(filter_config['type'])
        if not _filter:
            app_log.error('cannot find filter, {0}', filter_config)
            return False

        return _filter.filter(notification, filter_config)


class EsIndexRegexFilter(NotificationFilter):
    """
    Elasticsearch索引和type过滤器
    """

    def filter(self, notification, filter_config):
        """
        判断是否满足过滤条件
        :param notification:
        :param filter_config:
        :return:
        """
        if 'conditions' not in filter_config or not filter_config['conditions']:
            return True
        union_operator = filter_config.get('union_operator', 'and')
        return self.__match_conditions(notification, filter_config['conditions'], union_operator)


    def __match_conditions(self, notification, condition_list, union_operator):
        """
        判断是否符合所有匹配条件
        """
        for condition in condition_list:
            match_result = self.__match_single_condition(notification, condition)
            if union_operator == 'and' and not match_result:
                return False
            elif union_operator == 'or' and match_result:
                return True
        return True if union_operator == 'and' else False

    def __match_single_condition(self, notification, condition):
        """
        判断是否符合单个匹配条件
        """
        operator = condition['operator']
        match_result = False
        if condition['type'] == 'regex':
            # 正则表达式匹配条件
            field = condition.get('field')
            if not field:
                return True
            msg_text = self.__get_text(notification, field)
            match_result = True if search(condition['expression'], msg_text) else False
            match_result = not match_result if operator == 'not' else match_result
        return match_result


    def __get_text(self, notification, field):
        """
        获取要匹配的字段的值
        :param notification:
        :param field:
        :return:
        """
        return notification[field] if isinstance(notification, dict) and field in notification else ''


FILTERS_DICT = {'es_regex': EsIndexRegexFilter()}

notification_filter = NotificationFilter()