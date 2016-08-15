# coding=utf-8
from __future__ import absolute_import

from common.utils import get_dict_value_by_path
from common.loggers import debug_log, app_log
from common.configs import config
from common.data_parsers import data_parser
from river.destination import destination
from river.msg_filter import MessageFilter
from river.source import source
from river import get_river_key, do_msg_process_error

__author__ = 'liuzhaoming'


class MessageProcessor(object):
    """
    消息处理器，消费具体的消息
    """

    def __init__(self, river_config):
        self.river_config = river_config
        self.__init_config(river_config)

    def process(self, message):
        """
        处理消息
        """
        if not self.__match(message):
            return None
        message_parse_result = self.parse_message(message)
        app_log.info('Message_parse_result finish message={0} result={1}', message, message_parse_result)
        if None == message_parse_result:
            app_log.error(
                "The message parse result is None, there may be something wrong with message = {0}",
                message)
            return

        self.__do_data_flow(self.river_config, message_parse_result)

    def __match(self, message):
        """
        判断消息是否需要处理
        """
        return self.filter.filter(message) if self.filter else config.get_value(
            "consts/filter/default_match_result") == 'true'

    def __init_config(self, river_config):
        """
        初始化配置
        """
        filter_config = get_dict_value_by_path('notification/filter', river_config)
        self.filter = MessageFilter(filter_config) if filter_config else None

        # self.notification_type = get_dict_value_by_path('notification/type', river_config, 'MQ')
        # self.host = get_dict_value_by_path('notification/host', river_config)
        # self.topic = get_dict_value_by_path('notification/topic', river_config)
        # self.queue = get_dict_value_by_path('notification/queue', river_config)
        # if self.notification_type == 'MQ' and (not self.host or (not self.topic and not self.queue)):
            # Todo 此处需要处理，如果配置不合法，是抛出异常还是构造函数处理
        # app_log.error(
        # "Notification config is invalid, type is MQ, but host or topic is null, {0}", river_config)
        #     return

    def parse_message(self, message):
        """
        解析消息，目前只支持TextMessage
        :param message:
        :return:
        """
        data_parser_config = get_dict_value_by_path('notification/data_parser', self.river_config)
        if message.get('type') == 'pyactivemq.TextMessage':
            return self.__parse_text_message(message, data_parser_config)
        else:
            app_log.warning("The message is not TextMessage,{0}", message)
            return None

    @staticmethod
    def __parse_text_message(message, data_parser_config):
        """
        解析TextMessage
        :param message:
        :return:
        """
        return data_parser.parse(message.get('text'), data_parser_config)

    def __do_data_flow(self, river_config, pull_request_param):
        source_config = river_config.get('source', {})
        if source.is_iterable(source_config):
            self.__do_iteration_data_flow(river_config, pull_request_param)
        else:
            self.__do_single_data_flow(river_config, pull_request_param)

    @staticmethod
    def __parse_pull_response(river_config, pull_response):
        """
        解析从业务抓取回来的数据
        """
        pull_parser_fields = get_dict_value_by_path('source/response/fields', river_config)
        _pull_parser_values = {}
        if not pull_parser_fields or not pull_response:
            return pull_response
        debug_log.print_log('parse_pull_response pull_parser_fields is {0}, {1}', pull_parser_fields, pull_response)
        for field_key in pull_parser_fields:
            if pull_parser_fields[field_key] in pull_response:
                _pull_parser_values[field_key] = pull_response.get(pull_parser_fields[field_key])
        # _pull_parser_values = bind_dict_variable(pull_parser_fields, pull_response)
        debug_log.print_log('parse_pull_response result is {0}', _pull_parser_values)
        return _pull_parser_values

    @staticmethod
    def __do_iteration_data_flow(river_config, pull_request_param):
        """
        迭代处理数据流
        """
        if not source or not destination or not river_config:
            return
        has_next, page_from, size = True, 0, config.get_value('consts/source/default_iteration_get_size')
        source_config = get_dict_value_by_path('source', river_config)
        total = 0
        while has_next:
            for key in pull_request_param:
                pull_request_param[key].update({'page_from': page_from, 'page_size': size})
            debug_log.print_log('__do_iteration_data_flow iter page_from={0} , page_size={1}', page_from, size)
            pull_response = source.pull(source_config, pull_request_param)
            if pull_response is None:
                app_log.info('Pull response is None')
                break

            if isinstance(pull_response, tuple) or isinstance(pull_response, list):
                pull_response = pull_response[0]
            pull_parser_values = MessageProcessor.__parse_pull_response(river_config, pull_response)
            data = pull_parser_values['data']
            total = total if total else pull_parser_values['total']
            if page_from == 0:
                # 如果是第一次执行，支持清除掉数据目的地中数据
                destination.clear(river_config, data, pull_request_param)
            destination.push(river_config, data, pull_request_param)
            # pull_parser_values['']
            # cur_size = len(data) if isinstance(data, list) or isinstance(data, tuple) else 1
            page_from += 1
            cur_size = page_from * size
            has_next = cur_size < total


    @staticmethod
    def __do_single_data_flow(river_config, pull_request_param):
        """
        处理单次数据流
        """
        if not source or not destination or not river_config:
            return
        source_config = get_dict_value_by_path('source', river_config)
        pull_response = source.pull(source_config, pull_request_param)
        # if isinstance(pull_response, tuple) or isinstance(pull_response, list):
        #     pull_response = pull_response[0]
        pull_parser_values = MessageProcessor.__parse_pull_response(river_config, pull_response)
        if pull_parser_values is None:
            app_log.error('__do_single_data_flow cannot pull data from source, please check')
            return

        pull_parser_fields = get_dict_value_by_path('source/response/fields', river_config)
        destination.push(river_config,
                         pull_parser_values[
                             'data'] if pull_parser_fields and 'data' in pull_parser_values else pull_parser_values,
                         pull_request_param)


class MessageProcessorChain(object):
    """
    消息处理器责任链
    """

    def __init__(self, river):
        self.__int_config(river)

    def __int_config(self, river):
        """
        初始化配置
        :param river:
        :return:
        """
        self.processor_list = []
        self.notification_type = get_dict_value_by_path('notification/type', river, 'MQ')
        self.host = get_dict_value_by_path('notification/host', river)
        self.topic = get_dict_value_by_path('notification/topic', river)
        self.queue = get_dict_value_by_path('notification/queue', river)
        if self.notification_type == 'MQ':
            self.key = get_river_key(river)
            self._add_processor(river)
        else:
            # Todo 此处需要处理，如果配置不合法，是抛出异常还是构造函数处理
            app_log.error("Notification config is invalid, type is not support {0}", self.notification_type)

    def process(self, message):
        """
        处理消息
        :param message:
        :return:
        """
        for processor in self.processor_list:
            try:
                processor.process(message)
            except Exception as e:
                app_log.error("Process message has error, processor.test_river={0}, message={1}", e,
                              processor.river_config, message)
                do_msg_process_error(e)

    def _add_processor(self, river):
        """
        增加消息处理器
        :param river:
        :return:
        """
        branches = get_dict_value_by_path('branches', river)
        if branches:
            self.processor_list.extend(map(lambda river_branch: MessageProcessor(river_branch), branches))