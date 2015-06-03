# coding=utf-8
from suggest.notifications import suggest_notification


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

import unittest

from suggest.processors import suggest_processor
from common.configs import config


class TestProcessors(unittest.TestCase):
    def test_suggest_processor(self):
        suggest_config = config.get_value('suggest')[0]
        process_param = {'index': 'qmshop-test', 'type': 'QmShopProduct'}
        suggest_processor.process(suggest_config, process_param)

    def test_suggest_notification(self):
        suggest_notification.notify()


if __name__ == '__main__':
    unittest.main()