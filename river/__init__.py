# coding=utf-8
from __future__ import absolute_import

# This will make sure the app is always imported when
# Django starts so that shared_task will use this app.

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
    topic = get_dict_value_by_path('notification/topic', river) or topic
    queue = get_dict_value_by_path('notification/queue', river) or queue
    if notification_type == 'MQ':
        return COMBINE_SIGN.join((notification_type, host, 'topic', topic)) if topic else \
            COMBINE_SIGN.join((notification_type, host, 'queue', queue))
    else:
        return ""


from river import rivers
