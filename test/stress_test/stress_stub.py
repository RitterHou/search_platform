# coding=utf-8



def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../../'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../../../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()
import time

from elasticsearch import Elasticsearch

from river import get_river_key
from river.rivers import data_rivers
from test.stubs.mq_stub import MQStub
from common.connections import EsConnectionFactory


__author__ = 'liuzhaoming'


def send_active_mq_message(size):
    """
        批量发送MQ消息
        :return:
        """
    start_time = time.time()
    message_text_format = 'type=update;adminID=test;brand=search_platform_test;name=name{0};id={1};'
    mq_stub = MQStub('tcp://172.19.65.38:61616?wireFormat=openwire', 'topic.search_platform_test.update')
    text_message = mq_stub.producer_session.createTextMessage()
    for i in xrange(size):
        message_text = message_text_format.format(i, i)
        text_message.text = message_text
        mq_stub.producer.send(text_message)
    print 'spend {0}'.format(time.time() - start_time)


def create_serializable_mesage(text):
    return {'type': 'pyactivemq.TextMessage', 'text': text}


def test_add_message_processor():
    data_rivers.init_config()
    add_message_text = 'type=update;adminID=test;brand=search_platform_test;name=name{0};id={1};'
    add_message_text = add_message_text.format(1, 1)
    text_message = create_serializable_mesage(add_message_text)
    river_key = get_river_key({"notification": {"type": "MQ",
                                                "host": "tcp://172.19.65.38:61616?wireFormat=openwire",
                                                "topic": "topic.search_platform_test.update"}})
    message_process_chain = data_rivers.get_message_process_chain(river_key)
    if not message_process_chain:
        print('cannot find process chain by river_key : {0}'.format(river_key))
        return
    message_process_chain.process(text_message)


def test_es_capacity(size):
    connection = Elasticsearch('http://172.19.65.79:9200')
    es_start_time = time.time()
    for i in xrange(size):
        doc = {'adminID': 'test', 'brand': 'search_platform_test', 'name': 'name', 'id': i}
        bulk_body = [{"index": {"_index": 'search_platform-test-1.2.3', "_type": 'Product', "_id": str(i)}}, doc]
        connection.bulk(bulk_body)
    print('es spend time {0}'.format(time.time() - es_start_time))


def test_es_capacity_by_conn(size):
    connection = EsConnectionFactory.get_es_connection('http://172.19.65.79:9200')
    es_start_time = time.time()
    for i in xrange(size):
        doc = {'adminID': 'test', 'brand': 'search_platform_test', 'name': 'name', 'id': i}
        bulk_body = [{"index": {"_index": 'search_platform-test-1.2.3', "_type": 'Product', "_id": str(i)}}, doc]
        connection.bulk(bulk_body)
    print('es spend time {0}'.format(time.time() - es_start_time))


def test_es_capacity_by_sep(size):
    connection = EsConnectionFactory.get_es_connection('http://172.19.65.79:9200')

    for i in xrange(size):
        doc = {'adminID': 'test', 'brand': 'search_platform_test', 'name': 'name', 'id': i}
        bulk_body = [{"index": {"_index": 'search_platform-test-1.2.3', "_type": 'Product', "_id": str(i)}}, doc]
        es_start_time = time.time()
        connection.bulk(bulk_body)
        print('es spend time {0}'.format(time.time() - es_start_time))
        time.sleep(1)


if __name__ == '__main__':
    send_active_mq_message(15)