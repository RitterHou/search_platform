# coding=utf-8
"""
REST接口质量保证，主要通过Kafka消息重做
"""


def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()

import threading
import time
import ujson as json

import jsonpickle

from common.configs import config
from common.loggers import app_log
from common.msg_bus import message_bus, Event
from common.sla import rest_sla
from service import desc_request
from service.req_router import request_router


__author__ = 'liuzhaoming'


class RestQos(object):
    """
    Rest Qos
    """

    def __init__(self):
        self._kafka_topic = rest_sla.get_kafka_topic()
        self._redo_consumer_group = str(
            config.get_value('/consts/query/sla/rest_request_fail_consumer_redo_group') or 'redo_consumer_groups')
        self._consumer = self._kafka_topic.get_balanced_consumer(consumer_group=self._redo_consumer_group,
                                                                 auto_start=True,
                                                                 auto_commit_enable=True, managed=True,
                                                                 consumer_timeout_ms=1000, auto_commit_interval_ms=5000)
        self._consumer_status = 'stop'
        self._mutex = threading.Lock()
        message_bus.add_event_listener(Event.TYPE_REST_KAFKA_CONSUMER_START_STOP, self.process_consumer_operation_event)

    def process_consumer_operation_event(self, event):
        """
        处理kafka操作消息消费者
        :param event:
        :return:
        """
        app_log.info('RestQos receive consumer operation {0}', event)
        if not event.data:
            app_log.info('RestQos receive consumer operation event has no data')
            return

        if event.data.get('operation') == 'start':
            self._start_consumer()
        elif event.data.get('operation') == 'stop':
            self._stop_consumer()

    def _start_consumer(self):
        """
        启动kafka consumer
        :return:
        """
        if self._mutex.acquire():
            if self._consumer_status == 'stop':
                # self._consumer.start()
                self._consumer_status = 'start'
            self._mutex.release()

    def _stop_consumer(self):
        """
        停止kafka consumer
        :return:
        """
        if self._mutex.acquire():
            if self._consumer_status == 'start':
                # self._consumer.stop()
                self._consumer_status = 'stop'
            self._mutex.release()

    def process_kafka_message(self, message):
        """
        处理kafka message消息
        :param message: 格式为：
        message = {'timestamp': timestamp, 'request': jsonpickle.encode(request), 'exception': str(exception)}
        :return:
        """
        try:
            json_message = json.loads(message)
            timestamp = json_message['timestamp']
            request = jsonpickle.decode(json_message['request'])

            self.__handle_request(request, timestamp)

        except Exception as e:
            app_log.error('process kafka message fail {0}', e, message)

    @staticmethod
    def __handle_request(request, timestamp):
        req_handler = request_router.route(request)
        if req_handler:
            req_handler.handle(request, format, timestamp, redo=True)
        else:
            app_log.error('RestQos cannot find request handler {0}', desc_request(request))

    def start(self):
        while True:
            try:
                if self._consumer_status != 'start':
                    time.sleep(10)
                    continue
                message = self._consumer.consume(block=True)
                if message is None:
                    continue
                app_log.info('Consume kafka message offset {0}', message.offset)
                self.process_kafka_message(message.value)
            except Exception as e:
                app_log.error('RestQos run has error ', e)


if __name__ == '__main__':
    rest_qos = RestQos()
    rest_qos.start()


