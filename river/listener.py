# coding=utf-8
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
import Queue
import time
import threading

import pyactivemq
from pyactivemq import ActiveMQConnectionFactory
from pyactivemq import AcknowledgeMode
from common.msg_bus import message_bus, Event
from river.rivers import process_message
from common.configs import config
from common.loggers import debug_log, listener_log as app_log
from river import get_river_key
from common.data_parsers import data_parser
from common.sla import msg_sla
from common.utils import get_dict_value_by_path


__author__ = 'liuzhaoming'

message_queue = Queue.Queue(0)


def process_message_wrapper(_message_dict_list):
    """
    消息处理函数包装器
    :param _message_dict_list:
    :return:
    """
    for _message_dict in _message_dict_list:
        try:
            process_message.delay(_message_dict, _message_dict['river_key'])
        except Exception as e:
            app_log.error('process message error {0}', e, _message_dict)
class MQMessageListener(pyactivemq.MessageListener):
    """
    消息监听器，不处理消息，只负责转发消息到celery队列
    """

    def __init__(self, name, river_key, sla_cfg=None):
        pyactivemq.MessageListener.__init__(self)
        self.name = name
        self.river_key = river_key
        self.sla_cfg = sla_cfg or {}
        self.data_parser_config = get_dict_value_by_path('data_parser', self.sla_cfg)

    def onMessage(self, message):
        """
        消息监听器
        :param message:
        :return:
        """
        try:
            serial_message = self.__convert_message(message)
            app_log.info('Receive MQ message {0}, key={1}', serial_message, self.river_key)
            serial_message['river_key'] = self.river_key
            if serial_message:
                msg_sla.send_msg_to_queue(serial_message)
                # message_queue.put({'river_key': self.river_key, 'message': serial_message})
        except Exception as ex:
            app_log.exception(ex)

    def __convert_message(self, message):
        """
        转换消息，pyactivemq.TextMessage无法被python celery序列化
        :param message:
        :return:
        """
        if isinstance(message, pyactivemq.TextMessage):
            json_msg = {'type': 'pyactivemq.TextMessage', 'text': message.text}
            self.__get_sla_info(message.text, json_msg)
            return json_msg

        app_log.info('Message type not support {0}', message)
        return None
    def __get_sla_info(self, message_text, message_json):
        """
        获取MQ消息SLA相关配置
        """
        if message_text and self.data_parser_config and message_json:
            data_parse_result = data_parser.parse(message_text, self.data_parser_config)
            if data_parse_result and data_parse_result['fields'] and data_parse_result['fields']['adminId']:
                message_json['adminId'] = data_parse_result['fields']['adminId']
            if 'adminId' not in message_json:
                message_json['adminId'] = 'default'
        if 'redo' in self.sla_cfg:
            message_json['redo'] = self.sla_cfg['redo']
        else:
            message_json['redo'] = False


class ExceptionListener(pyactivemq.ExceptionListener):
    def __init__(self, host):
        pyactivemq.ExceptionListener.__init__(self)
        self.host = host

    """
    异常消息监听器
    """

    def onException(self, ex):
        app_log.exception('Receive Exception MQ message {0} {1}', self.host, ex)


class StompMessageListener(object):
    """
    stomp协议MQ消息监听器
    """

    def on_error(self, headers, message):
        app_log.info('Received MQ error {0}, {1}', headers, message)

    def on_message(self, headers, message):
        try:
            serial_message = self.__convert_message(message)
            app_log.info('Receive MQ message {0}', serial_message)
            if serial_message:
                message_queue.put({'river_key': self.river_key, 'message': serial_message})
        except Exception as ex:
            app_log.exception(ex)

    def __convert_message(self, headers, message):
        """
        转换消息，普通的message无法被python celery序列化
        :param headers:
        :param message:
        :return:
        """
        return {'type': 'pyactivemq.TextMessage', 'text': message}


class ListenerRegister(object):
    # 根据host存放的MQ connection 字典
    mq_conn_host_dic = {}

    # 根据key为river_key存放的MQ session 字典
    mq_consumer_topic_dic = {}

    # 从配置文件中获取的说有MQ目的地列表
    mq_notification_list = []

    # MQ session的clientID
    client_id = config.get_value('consts/notification/mq_client_id').encode('utf8')

    # 连接失败的MQ目的地字典
    fail_mq_notification_dict = {}

    def register_listeners(self):
        """
        初始化监听器
        :return:
        """
        # 初始化listener相关配置
        self.fail_mq_notification_dict = {}
        self.mq_notification_list = self.__get_all_notifications()
        map(self.__close_mq_session, self.mq_consumer_topic_dic.itervalues())
        map(self.__stop_mq_conn, self.mq_conn_host_dic.itervalues())
        self.mq_conn_host_dic = {}
        self.mq_consumer_topic_dic = {}
        map(self.__add_mq_listener, self.mq_notification_list)
        map(self.__start_mq_conn, self.mq_conn_host_dic.itervalues())

    def auto_reconnect(self):
        import time
        import thread

        def __reconnect():
            try:
                debug_log.print_log('Reconnect mq is start')
                reconnect_successful_notifications = {}
                reconnect_successful_hosts = set()
                for notification_key in self.fail_mq_notification_dict:
                    if self.__add_mq_listener(self.fail_mq_notification_dict[notification_key], True):
                        # 如果重连成功，需要记录下来
                        reconnect_successful_notifications[notification_key] = self.fail_mq_notification_dict[
                            notification_key]
                        host = self.fail_mq_notification_dict[notification_key].get('host')
                        reconnect_successful_hosts.add(host)
                # 如果重连成功，需要从失败列表中删除该记录
                for notification_key in reconnect_successful_notifications:
                    del self.fail_mq_notification_dict[notification_key]

                for host in reconnect_successful_hosts:
                    host_conn = self.mq_conn_host_dic.get(host)
                    if host_conn:
                        self.__start_mq_conn(host_conn)
            except Exception as ex:
                debug_log.print_log('Reconnect mq has exception {0}', ex.message)

        def __timer():
            interval = config.get_value('consts/notification/mq_reconnect_time') or 30
            while True:
                __reconnect()
                time.sleep(interval)

        thread.start_new_thread(__timer, ())
    def init_msg_handler(self):
        """
        初始化MQ消息处理器
        :return:
        """
        import time
        def handle_msg(is_vip):
            """
            处理消息
            """
            time_interval = 1
            while True:
                _start_time = time.time()
                msg_sla.process_msg(process_message_wrapper, is_vip)
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle admin vip({0}) msg spends {1}', is_vip, _cost_time)
                time_delta = time_interval - _cost_time
                if time_delta >= 0.01:
                    time.sleep(time_delta)
        def handle_redo_msg():
            """
            处理重做消息
            """
            time_interval = 10
            while True:
                _start_time = time.time()
                msg_sla.process_redo_msg(process_message_wrapper)
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle redo msg spends {0}', _cost_time)
                time_delta = time_interval - _cost_time
                if time_delta >= 1:
                    time.sleep(time_delta)
        def handle_check_msg_num():
            """
            """
            time.sleep(8)
            time_interval = 60
            while True:
                _start_time = time.time()
                msg_sla.check_msg_num()
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle check msg spends {0}', _cost_time)
                time_delta = time_interval - _cost_time
                if time_delta >= 1:
                    time.sleep(time_delta)
        if len(self.mq_conn_host_dic.values()) > 0:
            t1 = threading.Thread(target=handle_msg, args=(True,), name='Vip put thread')
            t1.setDaemon(True)
            t2 = threading.Thread(target=handle_msg, args=(False,), name='Experience put thread')
            t2.setDaemon(True)
            t3 = threading.Thread(target=handle_redo_msg, name='Handle redo msg thread')
            t3.setDaemon(True)
            t4 = threading.Thread(target=handle_check_msg_num, name='Handle check msg num thread')
            t4.setDaemon(True)
            t1.start()
            t2.start()
            t3.start()
            t4.start()


    @staticmethod
    def __get_all_notifications():
        """
        获取配置文件中的所有MQ配置项,不管是否重复，后面会进行过滤
        :return:
        """
        river_list = config.get_value('data_river/rivers')
        return filter(lambda notification: notification, map(lambda river: river.get('notification'), river_list))

    def __add_mq_listener(self, mq_notification, debug_print=False):
        """
        增加MQ监听器
        :param mq_notification:
        :param debug_print:
        :return:
        """
        try:
            if not debug_print:
                app_log.info("Add mq listener is called {0}", mq_notification)
            else:
                debug_log.print_log("Add mq listener is called {0}", mq_notification)

            mq_host = mq_notification.get('host').format(**config.get_value('consts/custom_variables'))
            mq_topic = mq_notification.get('topic')
            mq_queue = mq_notification.get('queue')
            notification_type = mq_notification.get('type', 'MQ')
            session_key = get_river_key({}, notification_type, mq_host, mq_topic, mq_queue)
            if mq_host in self.mq_conn_host_dic:
                conn = self.mq_conn_host_dic[mq_host]
            else:
                conn = ActiveMQConnectionFactory(mq_host.encode('utf8')).createConnection('', '', self.client_id)
                conn.exceptionListener = ExceptionListener(mq_host)
                self.mq_conn_host_dic[mq_host] = conn

            if session_key in self.mq_consumer_topic_dic:
                return True

            session = conn.createSession(AcknowledgeMode.AUTO_ACKNOWLEDGE)
            if mq_topic:
                topic = session.createTopic(mq_topic.encode('utf8'))
                subscriber = session.createDurableConsumer(topic, session_key.encode('utf8'), '', False)
            elif mq_queue:
                queue = session.createQueue(mq_queue.encode('utf8'))
                subscriber = session.createConsumer(queue, '')
            listener = MQMessageListener('listener {0}'.format(session_key), session_key,
                                         sla_cfg=get_dict_value_by_path('sla', mq_notification, {}))
            subscriber.messageListener = listener
            self.mq_consumer_topic_dic[session_key] = subscriber

            if not debug_print:
                app_log.info("Add mq session successfully {0}", session_key)
            else:
                debug_log.print_log("Add mq session successfully {0}", session_key)
            return True
        except Exception as ex:
            if not debug_print:
                app_log.exception(ex)
            else:
                debug_log.print_log("Add mq session has exception {0}", ex.message)
            if isinstance(ex, pyactivemq.CMSException):
                # 表示连接不上服务端，需要记录下来定时重连
                self.fail_mq_notification_dict[session_key] = mq_notification
            return False


    def __start_mq_conn(self, mq_conn):
        """
        启动MQ连接
        :param mq_conn:
        :return:
        """
        if mq_conn:
            app_log.info('Start mq connection {0}', mq_conn)
            try:
                mq_conn.start()
            except Exception as e:
                app_log.exception(e)

    def __stop_mq_conn(self, mq_conn):
        """
        注销MQ连接
        :param mq_conn:
        :return:
        """
        if mq_conn:
            app_log.info('Stop mq connection {0}', mq_conn)
            try:
                mq_conn.close()
            except Exception as e:
                app_log.exception(e)

    def __close_mq_session(self, mq_session):
        """
        关闭MQ session
        :param mq_session:
        :return:
        """
        if mq_session:
            try:
                mq_session.close()
            except Exception as e:
                app_log.exception(e)


if __name__ == '__main__':
    app_log.info('Listener start')

    register = ListenerRegister()
    register.register_listeners()
    register.auto_reconnect()
    register.init_msg_handler()

    # 注册配置数据变更消息监听器
    message_bus.add_event_listener(Event.TYPE_CONFIG_UPDATE, register.register_listeners)

    app_log.info('Listener register finish')

    while 1:
        try:
            time.sleep(60)
            # message_dict = message_queue.get(block=True)
            # msg_count += 1
            # delta_time = time.time() - start_time
            # if delta_time > 5:
            # msg_count = 0
            # start_time = time.time()
            # if msg_count > 10:
            # msg_count = 0
            # start_time = time.time()
            # app_log.info('River receive msg will be limited')
            # time.sleep(5 - delta_time)
            # process_message.delay(message_dict['message'], message_dict['river_key'])
        except Exception as e:
            app_log.exception(e)