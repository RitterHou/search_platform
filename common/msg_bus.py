# coding=utf-8
import json

import redis

from common.utils import get_client_id, get_function_params
from search_platform.settings import SERVICE_BASE_CONFIG
from common.loggers import app_log


__author__ = 'liuzhaoming'

client_id = get_client_id()
channel = SERVICE_BASE_CONFIG['message_bus_channel']


class Event(object):
    """
    事件基类
    """
    TYPE_SYNCHRONIZED_CONFIG = 'synchronized_config'
    TYPE_UPDATE_LOG_LEVEL = 'update_log_level'
    TYPE_PROCESS_LOGIN = 'process_login'
    TYPE_PROCESS_DESTROY = 'process_destroy'
    TYPE_CONFIG_UPDATE = 'config_update'
    TYPE_VIP_ADMIN_ID_UPDATE = 'vip_admin_ids_update'
    TYPE_VIP_ADMIN_PARAMS_UPDATE = 'admin_params_update'


    def __init__(self, type, source=None, destination='all', data=None):
        self._type = type
        self._source = source
        self._destination = destination
        self._data = data

    @property
    def type(self):
        """
        返回事件类型
        :return:
        """
        return self._type

    @property
    def data(self):
        """
        返回消息中携带的数据
        :return:
        """
        return self._data

    @property
    def source(self):
        """
        返回消息的源
        :return:
        """
        return self._source

    @property
    def destination(self):
        """
        返回消息的目的地
        :return:
        """
        return self._destination

    def __unicode__(self):
        return 'Event(type={0},source={1},destination={2},data={3})'.format(self._type, self._source, self._destination,
                                                                            self._data)


class EventDispatcher(object):
    """
    进程内事件分发、监听
    """

    def __init__(self):
        """
        初始化类
        """
        self._events = dict()


    def __del__(self):
        """
        清空所有event
        """
        self._events = None

    def has_listener(self, event_type, listener):
        """
        返回注册到event_type的listener
        """
        # Check for event type and for the listener
        if event_type in self._events:
            return listener in self._events[event_type]
        else:
            return False

    def dispatch_event(self, event):
        """
        分发event到所有关联的listener
        """
        app_log.info('Dispatch event {0}'.format(event))
        if event.type in self._events:
            listeners = self._events[event.type]

            for listener in listeners:
                try:
                    input_params = get_function_params(listener)
                    if not input_params or (len(input_params) == 1 and 'self' in input_params):
                        listener()
                    else:
                        listener(event)
                except Exception as e:
                    app_log.error('Dispatch event has error {0}', e, event)

    def add_event_listener(self, event_type, listener):
        """
        给某种事件类型添加listener
        """
        app_log.info('Add event listener with event_type={0}, listener={1}', event_type, listener)
        if not self.has_listener(event_type, listener):
            listeners = self._events.get(event_type, [])
            listeners.append(listener)
            self._events[event_type] = listeners

    def remove_event_listener(self, event_type, listener):
        """
        移出某种事件类型的listener
        """
        app_log.info('Remove event listener with event_type={0}, listener={1}', event_type, listener)
        if self.has_listener(event_type, listener):
            listeners = self._events[event_type]

            if len(listeners) == 1:
                del self._events[event_type]

            else:
                listeners.remove(listener)
                self._events[event_type] = listeners


class RedisSubscriber(object):
    def __init__(self, event_dispatcher):
        self.dispatcher = event_dispatcher

    def on_message(self, message):
        """
        Redis消息处理
        :param event:
        :return:
        """
        try:
            app_log.info('Receive redis message {0}', message)
            data = message['data']
            if not data:
                app_log.info('Redis message has no data')
                return

            # json.loads方法不支持单引号
            data = data.replace('\'', '"')
            message_body = json.loads(data)

            if 'destination' in message_body and (
                            message_body['destination'] == 'all' or message_body['destination'] == client_id):
                event = Event(**message_body)
                self.dispatcher.dispatch_event(event)
            else:
                app_log.info('Discard redis message {0} by client {1}', message, client_id)
        except Exception as e:
            app_log.error('On redis message has error {0}', e, message)


class MessageBus(object):
    event_dispatcher = EventDispatcher()
    subscriber = RedisSubscriber(event_dispatcher)
    has_start = False


    def __init__(self):
        self.start()

    def __del__(self):
        self.stop()

    def start(self):
        """
        启动消息总线服务初始化Redis连接，注册消息监听器
        :return:
        """
        try:
            app_log.info('Message Bus is starting')
            if self.has_start:
                app_log.info('Message Bus no need to start')
                return
            redis_host = SERVICE_BASE_CONFIG.get('redis')

            pool = redis.ConnectionPool.from_url(redis_host)
            self.__redis_conn = redis.Redis(connection_pool=pool)
            self.__pubsub = self.__redis_conn.pubsub()
            self.__pubsub.subscribe(**{channel: RedisSubscriber(self.event_dispatcher).on_message})
            self.__subscriber_thread = self.__pubsub.run_in_thread(sleep_time=1)
            self.has_start = True
            app_log.info('Message Bus start successfully, redis host={0}', redis_host)
        except Exception as e:
            app_log.error('Fail to start message bus', e)

    def stop(self):
        """
        停止消息总线服务
        :return:
        """
        try:
            app_log.info('Message Bus is stopping')
            if not self.has_start:
                app_log.info('Message Bus no need to stop')
                return
            if self.__subscriber_thread:
                self.__subscriber_thread.stop()
            if self.__pubsub:
                self.__pubsub.close()
            self.__redis_conn = None
            self.has_start = False
            app_log.info('Message Bus stop successfully')
        except Exception as e:
            app_log.error('Fail to stop message bus', e)


    def publish(self, type, destination='all', source=None, body=None, **kwargs):
        """
        发送Redis消息
        :param type:
        :param destination:
        :param source:
        :param body:
        :return:
        """
        app_log.info('Publish redis message {0} {1} {2} {3} {4}', type, destination, source, body, kwargs)
        if not type:
            app_log.info('Message is invalid ')
            return
        data = {'type': type, 'destination': destination, 'source': source, 'data': body}
        self.__redis_conn.publish(channel, data)

    def dispatch_event(self, event=None, **kwargs):
        """
        在进程内分发消息
        :param event:
        :param kwargs:
        :return:
        """
        app_log.info('Dispatch event {0} {1}', event, kwargs)
        if not event and 'type' in kwargs:
            event = Event(**kwargs)
        self.event_dispatcher.dispatch_event(event)

    def add_event_listener(self, event_type, listener):
        """
        增加线程内消息监听器
        :param event_type:
        :param listener:
        :return:
        """
        app_log.info('Add event listener with event_type={0}, listener={1}', event_type, listener)
        self.event_dispatcher.add_event_listener(event_type, listener)

    def remove_event_listener(self, event_type, listener):
        """
        移除线程内消息监听器
        :param event_type:
        :param listener:
        :return:
        """
        app_log.info('Remove event listener with event_type={0}, listener={1}', event_type, listener)
        self.event_dispatcher.remove_event_listener(event_type, listener)


message_bus = MessageBus()

if __name__ == '__main__':
    dict_test = {'aa': 'bb', 'c_key': 4}
    json_encode = json.dumps(dict_test)
    json_decode = json.loads('{"aa": "bb", "c_key": 4}')
    print json_encode
    print json_decode
    print json.loads('{"aa": "bb"}')

    def onMessage(message):
        print message

    pool = redis.ConnectionPool.from_url('redis://127.0.0.1:6379/2')
    redis_conn = redis.Redis(connection_pool=pool)
    pubsub = redis_conn.pubsub()
    # self.__pubsub.subscribe(**{channel: RedisSubscriber(self.event_dispatcher).on_message})
    pubsub.subscribe(**{'111': onMessage})
    data = {'type': 'config_update', 'destination': 'all', 'data': 'test'}
    data = {'type': 'update_log_level', 'destination': 'all', 'data': {"debug": "WARNING"
        , "interface": "WARNING", "app": "WARNING", "query": "WARNING"
    }}

    redis_conn.publish(channel, data)
