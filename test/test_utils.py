# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'
import logging

from common.utils import init_django_env


def get_log_file_name(log_name):
    logger = logging.getLogger(log_name)
    for handler in logger.handlers:
        if handler.name == 'logfile':
            break
    file_name = handler.baseFilename
    return file_name


def clear_log(log_name):
    """
    清空日志文件内容
    :param log_name:
    :return:
    """
    file_name = get_log_file_name(log_name)
    f = open(file_name, 'r+')
    f.seek(0)
    f.truncate(0)


def statistic_log(log_name, pattern):
    """
    对日志文件进行统计
    :param log_name:
    :param pattern:
    :return:
    """
    file_name = get_log_file_name(log_name)
    f = open(file_name, 'r')
    line_list = filter(lambda line: line.find(pattern) > -1, f.readlines())
    return len(line_list)


init_django_env()
clear_log('root')