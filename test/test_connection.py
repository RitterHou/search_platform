# -*- coding: utf-8 -*-
from dubbo_client import ApplicationConfig, ZookeeperRegistry

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


# init_django_env()

# import unittest
#
#
# class TestDubbo(unittest.TestCase):
#     def test_dubbo(self):
#         pass


if __name__ == '__main__':
    host = '192.168.65.183:2181,192.168.65.184:2181,192.168.65.185:2181'
    registry = ZookeeperRegistry(host, ApplicationConfig('search_platform'))

