# -*- coding: utf-8 -*-
from __future__ import absolute_import

__author__ = 'liuzhaoming'


def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()

import time
import unittest

from river.rivers import data_rivers
from test.stubs.mq_stub import MQStub
from test.test_utils import *
from search_platform.celery_config import app
from river import get_river_key
from common.utils import *
from river.rivers import process_message


@app.task
def test_fun():
    print 'test is called'


class TestRiver(unittest.TestCase):
    """
    river.rivers模块测试用例
    """

    def setUp(self):
        clear_log('root')
        # config.set_config_file_path(BASE_DIR + '/test/test_river/config1.json')
        # config.refresh()
        # self.mq_stub = MQStub('tcp://0.0.0.0:61616?wireFormat=openwire', 'search_test')

    def test_mq_message_listener(self):
        data_rivers.init_config()
        msg_text = 'Hello, this is test mq message!'
        num = 5
        # self.mq_stub.produce_msg(msg_text, num)
        print 'send finish'
        test_fun.delay()
        while True:
            time.sleep(300)
        receive_message_num = statistic_log('root', 'ProductMessageListener.onMessage')
        self.assertEqual(num, receive_message_num, 'The number of received message is not equals with the send')

    def test_msg_processor(self):
        data_rivers.init_config()
        text_message = self.create_serializable_mesage('Hello, this is a test message')
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_add_message_processor(self):
        data_rivers.init_config()
        add_message_text = 'msg_type=add;adminID=a00000001;ids=12,1999,qianmi123456,33,of1540;'
        text_message = self.create_serializable_mesage(add_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_update_message_processor(self):
        data_rivers.init_config()
        add_message_text = 'msg_type=add;adminID=update;ids=qianmi888;add_field=update;'
        update_message_text = 'msg_type=update;adminID=update;ids=qianmi888;add_field=update;'
        text_message = self.create_serializable_mesage(add_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)
        update_text_message = self.mq_stub.create_text_msg(update_message_text)
        message_process_chain.process(update_text_message)

    def test_delete_message_processor(self):
        data_rivers.init_config()
        delete_message_text = 'msg_type=delete;adminID=update;ids=qianmi888;add_field=update;'
        text_message = self.create_serializable_mesage(delete_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_init_message_processor(self):
        data_rivers.init_config()
        delete_message_text = 'msg_type=init;adminID=update;ids=qianmi888;add_field=update;'
        text_message = self.create_serializable_mesage(delete_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_add_message_processor_mq_celery(self):
        data_rivers.init_config()
        add_message_text = 'msg_type=add;adminID=a00000001;ids=test_add_message_processor_mq_celery_1;'
        self.mq_stub.produce_msg(add_message_text, 1)

    def test_celery(self):
        data_rivers.init_config()
        delete_message_text = 'msg_type=add;adminID=a00000001;ids=test_celery_1;add_field=test_celery;'
        text_message = self.mq_stub.create_text_msg(delete_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        process_message.delay({'text': text_message.text, 'type': 'pyactivemq.TextMessage'}, river_key)

    def create_serializable_mesage(self, text):
        return {'type': 'pyactivemq.TextMessage', 'text': text}


    def test_dubbo_message_processor(self):
        data_rivers.init_config()
        message_text = '{"chainMasterId":"A892107","ids":"p5780:g22462;","operation":"stock","sys":2,"type":"update"}'
        text_message = self.create_serializable_mesage(message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://172.19.66.39:61616?wireFormat=openwire",
                                                    "topic": "t.qmpc.ItemChange", }})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_hybrid_message_delete(self):
        data_rivers.init_config()
        message_text = '{"chainMasterId":"A892107","deleteSkuIds":"g22462;","operation":"edit","sys":2,"type":"update"}'
        text_message = self.create_serializable_mesage(message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://172.19.65.38:61616?wireFormat=openwire",
                                                    "topic": "t.qmpc.ItemChange", }})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)


    def test_send_init_message(self):
        mq_stub = MQStub('tcp://172.19.66.39:61616?wireFormat=openwire', 't.qmpc.Init')
        mq_stub.produce_msg('{\"chainMasterId\":\"A857673\"}')

    def test_init_message_process(self):
        data_rivers.init_config()
        message_text = '{\"chainMasterId\":\"A857673\"}'
        text_message = self.create_serializable_mesage(message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://172.19.66.39:61616?wireFormat=openwire",
                                                    "topic": "t.qmpc.Init", }})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def test_send_dubbo_message_message(self):
        data_rivers.init_config()
        message_text = '{"chainMasterId":"A892107","ids":"p5780:g22462;","operation":"stock","sys":2,"type":"update"}'
        mq_stub = MQStub('tcp://172.19.65.38:61616?wireFormat=openwire', 't.qmpc.ItemChange')
        mq_stub.produce_msg(message_text)


    def test_send_message_batch(self):
        data_rivers.init_config()
        add_message_text = 'msg_type=add;adminID=a00000001;ids=test_send_message_batch_uuu'
        for i in xrange(10):
            print "test_send_message_batch iter " + str(i)
            self.mq_stub.produce_msg(add_message_text + str(i) + ';', 1)


    def test_send_max_message(self):
        data_rivers.init_config()
        add_message_text = 'msg_type=add;adminID=a00000001;ids=test_send_message_batch_123456'
        text_message = self.create_serializable_mesage(add_message_text)
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://0.0.0.0:61616?wireFormat=openwire",
                                                    "topic": "search_test"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        import time

        start_time = time.time()
        for i in xrange(1000):
            message_process_chain.process(text_message)
        print (time.time() - start_time)


    def test_gonghuo_init_processor(self):
        data_rivers.init_config()
        text_message = {'text': '{method=init}', 'type': 'pyactivemq.TextMessage'}
        river_key = get_river_key({"notification": {"type": "MQ",
                                                    "host": "tcp://172.19.65.38:61616?wireFormat=openwire",
                                                    "topic": "VirtualTopic.gavin.category.operate"}})
        message_process_chain = data_rivers.get_message_process_chain(river_key)
        if not message_process_chain:
            logger.error('cannot find process chain by river_key : {0}'.format(river_key))
            return
        message_process_chain.process(text_message)

    def tearDown(self):
        pass
        # self.mq_stub.destroy()

    def test_gonghuo_init(self):
        mq_stub = MQStub('tcp://172.19.65.38:61616?wireFormat=openwire', 'VirtualTopic.gavin.category.operate')
        mq_stub.produce_msg('{method=init}')


if __name__ == '__main__':
    unittest.main()
    # test = TestRiver()
    # test.setUp()
    # test.test_msg_processor()