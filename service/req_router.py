# -*- coding: utf-8 -*-
from common.configs import config
from common.msg_bus import message_bus, Event
from common.loggers import query_log as app_log
from service import desc_request
from service.req_handler import RequestHandler

__author__ = 'liuzhaoming'


class RequestRouter(object):
    """
    HTTP请求路由器
    """

    def __init__(self):
        self.init_config()
        # 增加配置数据监听器
        message_bus.add_event_listener(Event.TYPE_CONFIG_UPDATE, self.init_config)

    def init_config(self):
        """
        根据配置数据初始化HTTP请求路由器
        :return:
        """
        self.__handler_list = None
        chain = config.get_value('query/chain')
        if not chain or len(chain) == 0:
            app_log.error("Chain config is invalid")
            return

        self.__handler_list = map(lambda handler_config: RequestHandler(handler_config), chain)

    def route(self, request):
        """
        将HTTP请求路由到指定的处理器
        :param request:
        :return:
        """
        for handler in self.__handler_list:
            if handler.match(request):
                return handler

        app_log.error('Cannot route http request : {0}', desc_request(request))


request_router = RequestRouter()