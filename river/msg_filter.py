# coding=utf-8
from re import search


__author__ = 'liuzhaoming'


class MessageFilter(object):
    """
    消息过滤器，判断消息是否需要处理
    """

    def __init__(self, filter_config):
        self.__init_config(filter_config)

    def filter(self, message):
        """
        过滤消息，如果消息符合过滤条件返回True，反之False
        """
        return self.__match_msg_type(message) and self.__match_conditions(message)

    def __init_config(self, filter_config):
        """
        根据配置文件初始化过滤器配置
        """
        self.filter_config = filter_config
        self.union_operator = filter_config.get('union_operator', 'and')
        self.msg_type = filter_config.get('msg_type', 'TextMessage')
        conditions = filter_config.get('conditions', ())
        self.condition_list = map(self.__init_condition, conditions)

    @staticmethod
    def __init_condition(condition_config):
        """
        初始化单个过滤条件
        """
        operator = condition_config.get('operator', 'is')
        condition_type = condition_config.get('type', 'regex')
        result = {'operator': operator, 'type': condition_type}
        if condition_type == 'regex':
            expression = condition_config.get('expression', '[\d\D]*')
            result.update(expression=expression)
        return result

    def __match_msg_type(self, message):
        """
        判断消息类型是否匹配
        """
        if self.msg_type == 'TextMessage':
            return message.get('type') == 'pyactivemq.TextMessage'
        return False

    def __match_conditions(self, message):
        """
        判断是否符合所有匹配条件
        """
        for condition in self.condition_list:
            match_result = self.__match_single_condition(message, condition)
            if self.union_operator == 'and' and not match_result:
                return False
            elif self.union_operator == 'or' and match_result:
                return True
        return True if self.union_operator == 'and' else False

    def __match_single_condition(self, message, condition):
        """
        判断是否符合单个匹配条件
        """
        operator = condition['operator']
        match_result = False
        if condition['type'] == 'regex':
            # 正则表达式匹配条件
            msg_text = self.__get_text(message)
            match_result = True if search(condition['expression'], msg_text) else False
            match_result = not match_result if operator == 'not' else match_result
        return match_result

    @staticmethod
    def __get_text(message):
        """
        获取消息中得文本
        """
        return message.get('text') if message.get('type') == 'pyactivemq.TextMessage' else str(message)