# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

import time

from django.test import TestCase

from test.stubs.mq_stub import MQStub
from test.test_utils import *
from common.configs import config
from search_platform.settings import BASE_DIR
from river.rivers import data_rivers


class RiverTestCase(TestCase):
    def setUp(self):
        clear_log('root')
        config.set_config_file_path(BASE_DIR + '/test/test_river/config1.json')
        config.refresh()
        self.mq_stub = MQStub('tcp://0.0.0.0:61616?wireFormat=openwire', 'search_test')

    def tearDown(self):
        self.mq_stub.destroy()

    def test_mq_message_listener(self):
        data_rivers.init_config()
        msg_text = 'Hello, this is test mq message!'
        num = 5
        self.mq_stub.produce_msg(msg_text, num)
        print 'send finish'
        while True:
            time.sleep(300)
        receive_message_num = statistic_log('root', 'ProductMessageListener.onMessage')
        self.assertEqual(num, receive_message_num, 'The number of received message is not equals with the send')
