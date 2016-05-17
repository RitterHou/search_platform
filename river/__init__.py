# coding=utf-8
from __future__ import absolute_import

# This will make sure the app is always imported when
# Django starts so that shared_task will use this app.
from httplib import HTTPException
from dubbo_client import DubboClientError
from elasticsearch import ConnectionTimeout, ConnectionError, SSLError, NotFoundError, ConflictError, RequestError
from elasticsearch.exceptions import AuthenticationException, AuthorizationException
from common.configs import config
from common.exceptions import MsgHandlingFailError

from common.utils import get_dict_value_by_path, COMBINE_SIGN

__author__ = 'liuzhaoming'


def get_river_key(river={}, notification_type=None, host=None, topic=None, queue=None):
    """
    获取数据流的标识key
    :param river:
    :return:
    """
    notification_type = get_dict_value_by_path('notification/type', river, 'MQ') or notification_type
    host = get_dict_value_by_path('notification/host', river) or host
    host = host.format(**config.get_value('consts/custom_variables'))
    topic = get_dict_value_by_path('notification/topic', river) or topic
    queue = get_dict_value_by_path('notification/queue', river) or queue
    if notification_type == 'MQ':
        return COMBINE_SIGN.join((notification_type, host, 'topic', topic)) if topic else \
            COMBINE_SIGN.join((notification_type, host, 'queue', queue))
    else:
        return ""


def do_msg_process_error(error):
    """
    处理消息处理失败异常，主要是异常向上面传递
    :param error:
    :return:
    """
    if isinstance(error, MsgHandlingFailError):
        raise error
    elif isinstance(error, (
            ConnectionError, SSLError, NotFoundError, ConflictError, RequestError, AuthenticationException,
            AuthorizationException)):
        raise MsgHandlingFailError(MsgHandlingFailError.ES_ERROR)
    elif isinstance(error, ConnectionTimeout):
        raise MsgHandlingFailError(MsgHandlingFailError.ES_READ_TIMEOUT)
    elif isinstance(error, DubboClientError):
        raise MsgHandlingFailError(MsgHandlingFailError.DUBBO_ERROR)
    elif isinstance(error, HTTPException):
        raise MsgHandlingFailError(MsgHandlingFailError.HTTP_ERROR)
from river import rivers
