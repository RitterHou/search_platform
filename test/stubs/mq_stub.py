# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

from pyactivemq import ActiveMQConnectionFactory


class MQStub(object):
    def __init__(self, host, topic_name):
        self.host = host
        self.topic_name = topic_name
        self.conn = ActiveMQConnectionFactory(host).createConnection()
        self.producer_session = self.conn.createSession()
        topic = self.producer_session.createTopic(topic_name)
        self.producer = self.producer_session.createProducer(topic)
        self.conn.start()

    def produce_msg(self, msg_text, times=1):
        def send(message):
            self.producer.send(message)

        """
        生产消息
        :param msg_text:
        :return:
        """
        text_message = self.producer_session.createTextMessage()
        for i in xrange(times):
            # print i
            text_message.text = msg_text + " |||||| number=" + str(i)
            # thread.start_new_thread(send, [text_message])
            self.producer.send(text_message)


    def create_text_msg(self, msg_text):
        text_message = self.producer_session.createTextMessage()
        text_message.text = msg_text
        return text_message


    def destroy(self):
        self.conn.close()


# mq_stub = MQStub('tcp://0.0.0.0:61616?wireFormat=openwire', 'search_test')
# mq_stub.produce_msg('test message', 1)


