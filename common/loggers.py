# coding=utf-8
import logging
import sys

import ujson as json
from itertools import chain

import os

__author__ = 'liuzhaoming'


# 日志等级
LOGGER_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
_error_logger = logging.getLogger('error')


def get_caller_info(level=2):
    f = sys._getframe()
    for i in xrange(level):
        if f.f_back is not None:
            f = f.f_back
        else:
            break
    line_number = f.f_lineno  # 获取行号
    file_name = f.f_code.co_filename
    file_strs = os.path.split(file_name)
    if len(file_strs) == 2:
        file_name = file_strs[1]
    return '{0}[line:{1}]'.format(file_name, line_number)


class AppLog(object):
    def __init__(self, logger_name='app'):
        self.logger = logging.getLogger(logger_name)

    def info(self, message, *args):
        try:
            if not message:
                return
            if args:
                message = message.format(*args)
            message = ' '.join((get_caller_info(), message))
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info(message)

    def warning(self, message, *args):
        try:
            if not message:
                return
            if args:
                message = message.format(*args)
            message = ' '.join((get_caller_info(), str(message)))
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.warning(message)

    def error(self, message, error=None, *args):
        try:
            if not message:
                return
            if error and not isinstance(error, Exception):
                args = chain([error], args)
            if args:
                message = message.format(*args)
            message = ' '.join((get_caller_info(), message))
        except Exception as e:
            self.logger.exception(e)
        finally:
            # self.logger.error(message)
            _error_logger.error(message)
            if error and isinstance(error, Exception):
                # self.logger.exception(error)
                _error_logger.exception(error)

    def exception(self, error):
        try:
            if error:
                # self.logger.exception(error)
                _error_logger.exception(error)
        except Exception as e:
            self.logger.exception(e)


class InterfaceLog(object):
    """
    打印接口交互日志
    """
    logger = logging.getLogger('interface')

    def __init__(self):
        self.__has_init = False

    def init_config(self):
        # 消除循环依赖
        from msg_bus import message_bus, Event

        self.update_log_level()
        message_bus.add_event_listener(Event.TYPE_UPDATE_LOG_LEVEL, self.update_log_level)
        self.__has_init = True

    def update_log_level(self):
        from common.configs import config

        self.log_level = config.get_value('consts/logger/interface/level')
        self.int_log_level = logging._levelNames[self.log_level]

    def print_log(self, message, *args):
        try:
            if not self.__has_init:
                self.init_config()
            if not self.logger.isEnabledFor(self.int_log_level) or not message:
                return
            if isinstance(message, dict):
                message['message'] = ' '.join((get_caller_info(), message.get('message') or ''))
                message = json.dumps(message)
            elif args:
                message = message.format(*args)
                message = ' '.join((get_caller_info(), message))
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info(message)
    def print_error(self, message, error, *args):
        if isinstance(message, dict):
            message['message'] = ' '.join((get_caller_info(), message.get('message') or ''))
            message['exceptionMsg'] = str(error)
            message = json.dumps(message)
        elif args:
            message = message.format(*args)
            message = ' '.join((get_caller_info(), message))
        self.logger.error(message)


class DebugLog(object):
    """
    打印接口交互日志
    """
    logger = logging.getLogger('debug')

    # python 连接mq的lib有多个，后续可能不使用pyactivemq
    try:
        import pyactivemq
    except ImportError as e:
        _error_logger.exception(e)

    def __init__(self):
        self.__has_init = False

    def init_config(self):
        from msg_bus import message_bus, Event
        # 消除循环依赖
        self.update_log_level()
        message_bus.add_event_listener(Event.TYPE_UPDATE_LOG_LEVEL, self.update_log_level)
        self.__has_init = True

    def update_log_level(self):
        from common.configs import config

        self.log_level = config.get_value('consts/logger/debug/level')
        self.int_log_level = logging._levelNames[self.log_level]


    def debug(self, msgs):
        """
        日志打印，打印函数的入参和出参
        :param log_level:
        :param msgs:
        :return:
        """

        def decorator(function):
            def new_func(*args, **kwargs):
                if not self.__has_init:
                    self.init_config()
                # self.__log(self.int_log_level, msgs, 'begin with the params:', *args, **kwargs)
                result = function(*args, **kwargs)
                # self.__log(self.int_log_level, msgs, 'finish with the result:', result)
                return result

            return new_func

        return decorator

    def print_log(self, message, *args):
        """
        打印debug日志，需要通过代码调用
        :param message:
        :param args:
        :return:
        """
        try:
            if not self.__has_init:
                self.init_config()
            if not self.logger.isEnabledFor(self.int_log_level) or not message:
                return
            if args:
                message = message.format(*args)
            message = ' '.join((get_caller_info(3), message))

            self.logger.info(message)
        except Exception as e:
            self.logger.exception(e)


    def __log(self, int_log_level, *args, **kwargs):
        if not self.logger.isEnabledFor(int_log_level):
            return

        try:
            str_args = map(lambda arg: str(arg), args)
            str_kwargs = map(lambda arg: str(arg), kwargs.iteritems())
            msg = ' '.join((get_caller_info(3), ' '.join(str_args), ' '.join(str_kwargs)))
        except Exception as e:
            self.logger.exception(e)
        finally:
            # self.logger.getEffectiveLevel()
            if int_log_level == 10:
                self.logger.debug(msg)
            elif int_log_level == 20:
                self.logger.info(msg)
            elif int_log_level == 30:
                self.logger.warning(msg)
            elif int_log_level == 40:
                self.logger.error(msg)

    def __desc_obj(self, obj):
        """
        对象详细描述，包含属性，主要用于调试
        :param obj:
        :return:
        """

        if isinstance(obj, pyactivemq.TextMessage):
            return '[{0} : {1}]'.format(str(obj), obj.text)
        if not obj or isinstance(obj, str) or isinstance(obj, bool) or isinstance(obj, list) or isinstance(obj, tuple) \
                or isinstance(obj, dict) or isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, bytes) \
                or isinstance(obj, unicode) or isinstance(obj, set):
            return str(obj)
        return '[{0} : {1}]'.format(str(obj), obj.__dict__ if hasattr(obj, '__dict__') else '')


app_log = AppLog()
debug_log = DebugLog()
interface_log = InterfaceLog()
listener_log = AppLog('listener')
query_log = AppLog('query')


def test():
    get_caller_info(2)


@debug_log.debug('kkk')
def test1():
    test()


if __name__ == '__main__':
    import time

    start_time = time.time()
    test1()
    app_log.error("a {0} b {1}", 'aaaaaa', 'bbbbbbb')
    print("spend {0}".format(time.time() - start_time))



