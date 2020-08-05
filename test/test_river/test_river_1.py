# -*- coding: utf-8 -*-
"""
用于测试后端的接口
"""
from river import get_river_key
from river.rivers import data_rivers


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

_message = '{"storeId":"A5722162","storeType":"A5722162","operation":"delete","cids":"R17042010881468"}'
_type = 'MQ'
_host = 'failover:(tcp://192.168.65.182:61616,tcp://192.168.65.181:61616,tcp://192.168.65.180:61616)?randomize=false'
_topic = 't.uc.retail.change'


def create_serializable_mesage(text):
    return {'type': 'pyactivemq.TextMessage', 'text': text}


def test_msg_processor():
    data_rivers.init_config()
    text_message = create_serializable_mesage(_message)
    river_key = get_river_key({'notification': {
        'type': _type,
        'host': _host,
        'topic': _topic
    }})
    message_process_chain = data_rivers.get_message_process_chain(river_key)
    if not message_process_chain:
        print ('cannot find process chain by river_key : {0}'.format(river_key))
        return
    message_process_chain.process(text_message)


if __name__ == '__main__':
    test_msg_processor()
