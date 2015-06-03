# -*- coding: utf-8 -*-

import time

from common.utils import COMBINE_SIGN
from common.loggers import app_log, debug_log
from common.configs import config
from common.msg_bus import message_bus, Event
from search_platform.celery_config import app
from river.processor import MessageProcessorChain
from river import get_river_key


__author__ = 'liuzhaoming'

LIST_SEP_SIGN = ';'


@app.task(bind=True)
def process_message(self, message, river_key):
    start_time = time.time()
    app_log.info('Celery process_message ia called  {0}  {1}', message, river_key)
    try:
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            app_log.error('Cannot find process chain by river_key : {0}', river_key)
            return

        message_process_chain.process(message)
        debug_log.print_log('Celery process message spend {0}', time.time() - start_time)
    except Exception as e:
        app_log.error('Process_message has error, message={0}, river_key={1}', e, message, river_key)


class DataRivers(object):
    # 处理器责任链字典，key为river_key
    processor_chain_dict = {}

    def __init__(self):
        self.init_config()
        # 添加配置信息变更事件监听器
        message_bus.add_event_listener(Event.TYPE_CONFIG_UPDATE, self.init_config)

    def init_config(self):
        # 初始化处理器责任链相关配置
        self.__init_processor_chain_config()

    def get_fetcher(self, admin_id, res_type):
        return self.fetcher_dict.get(admin_id + COMBINE_SIGN + res_type)

    def get_message_process_chain(self, river_key):
        """
        获取消息处理器链
        :param river_key:
        :return:
        """
        return self.processor_chain_dict.get(river_key)

    def __init_processor_chain_config(self):
        """
        初始化消息处理器责任链相关配置
        :return:
        """
        self.processor_chain_dict = {}
        river_list = config.get_value('data_river/rivers')
        for river in river_list:
            river_key = get_river_key(river)
            if river_key not in self.processor_chain_dict:
                self.processor_chain_dict[river_key] = MessageProcessorChain(river)
            self.processor_chain_dict[river_key].add_processor(river)


data_rivers = DataRivers()

if __name__ == '__main__':
    data_rivers.init_config()