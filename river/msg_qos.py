# coding=utf-8
"""
msq qos 消息转发
"""
def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os
    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'
    import django
    django.setup()
init_django_env()
import threading
import time
import sys
from common.loggers import app_log, debug_log
from common.sla import msg_sla
from river.rivers import process_message
from common.distributed_locks import zk_lock_store
__author__ = 'liuzhaoming'

def process_message_wrapper(_message_dict_list):
    """
    消息处理函数包装器
    :param _message_dict_list:
    :return:
    """
    for _message_dict in _message_dict_list:
        try:
            app_log.info('begin send msg to celery {0}', _message_dict)
            process_message.delay(_message_dict, _message_dict['river_key'])
            # process_message('', _message_dict, _message_dict['river_key'])
        except Exception as e:
            app_log.error('process message error {0}', e, _message_dict)
class MsgQos(object):
    def __init__(self):
        pass
    def start_vip_msg_handler(self):
        """
        启动VIP用户消息处理
        :return:
        """
        vip_msg_handler_thread = threading.Thread(target=self._handle_msg, args=(True,), name='Vip msg handler thread')
        vip_msg_handler_thread.setDaemon(True)
        vip_msg_handler_thread.start()
    def start_experience_msg_handler(self):
        """
        启动体验用户消息处理
        :return:
        """
        experience_msg_handler_thread = threading.Thread(target=self._handle_msg, args=(False,),
                                                         name='Experience msg handler thread')
        experience_msg_handler_thread.setDaemon(True)
        experience_msg_handler_thread.start()
    def start_vip_redo_msg_handler(self):
        """
        启动VIP消息失败消息重做
        :return:
        """
        vip_redo_msg_handler_thread = threading.Thread(target=self._handle_redo_msg, args=(True,),
                                                       name='Vip redo msg handler thread')
        vip_redo_msg_handler_thread.setDaemon(True)
        vip_redo_msg_handler_thread.start()
    def start_experience_redo_msg_handler(self):
        """
        启动体验用户失败消息重做
        :return:
        """
        experience_redo_msg_handler_thread = threading.Thread(target=self._handle_redo_msg, args=(False,),
                                                              name='Experience redo msg handler thread')
        experience_redo_msg_handler_thread.setDaemon(True)
        experience_redo_msg_handler_thread.start()
    def start_check_msg_num(self):
        """
        启动消息队列阈值检查
        :return:
        """
        check_msg_num_thread = threading.Thread(target=self._handle_check_msg_num, name='Check msg num thread')
        check_msg_num_thread.setDaemon(True)
        check_msg_num_thread.start()
    def start_check_final_msg_num(self):
        """
        启动最终失败消息队列阈值检查
        :return:
        """
        check_final_msg_num_thread = threading.Thread(target=self._handle_check_final_msg_num,
                                                      name='Check final msg num thread')
        check_final_msg_num_thread.setDaemon(True)
        check_final_msg_num_thread.start()
    def _handle_msg(self, is_vip):
        """
        处理消息
        :param is_vip
        """
        time_interval = 1
        while True:
            _start_time = time.time()
            try:
                msg_sla.process_msg(process_message_wrapper, is_vip)
                _cost_time = time.time() - _start_time
                app_log.info('handle admin vip({0}) msg spends {1}', is_vip, _cost_time)
            except Exception as e:
                app_log.error('handle vip({0}) msg has error, {0}', is_vip, e)
            finally:
                time_delta = time_interval - (time.time() - _start_time)
                if time_delta >= 0.01:
                    time.sleep(time_delta)
    def _handle_redo_msg(self, is_vip):
        """
        处理重做消息
        """
        time_interval = 10
        while True:
            _start_time = time.time()
            try:
                msg_sla.process_redo_msg(process_message_wrapper, is_vip)
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle vip({0}) redo msg spends {1}', is_vip, _cost_time)
            except Exception as e:
                app_log.error('handle vip({0}) redo msg has error, {0}', is_vip, e)
            finally:
                time_delta = time_interval - (time.time() - _start_time)
                if time_delta >= 1:
                    time.sleep(time_delta)
    def _handle_check_msg_num(self):
        """
        检查消息队列消息数目
        """
        time.sleep(8)
        time_interval = 60
        while True:
            _start_time = time.time()
            try:
                msg_sla.check_msg_num()
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle check msg spends {0}', _cost_time)
            except Exception as e:
                app_log.error('handle check msg num has error, ', e)
            finally:
                time_delta = time_interval - (time.time() - _start_time)
                if time_delta >= 1:
                    time.sleep(time_delta)
    def _handle_check_final_msg_num(self):
        """
        检查最终消息队列数目
        :return:
        """
        time.sleep(25)
        time_interval = 60
        while True:
            _start_time = time.time()
            try:
                msg_sla.check_final_msg_num()
                _cost_time = time.time() - _start_time
                debug_log.print_log('handle check final msg spends {0}', _cost_time)
            except Exception as e:
                app_log.error('handle check final msg num has error, ', e)
            finally:
                time_delta = time_interval - (time.time() - _start_time)
                if time_delta >= 1:
                    time.sleep(time_delta)
if __name__ == '__main__':
    if len(sys.argv) < 2 or sys.argv[1] not in ('vip', 'experience'):
        app_log.error('Msg qos app start param is not valid {0}', sys.argv)
        raise SystemError
    msg_qos = MsgQos()
    lock_task_name = '{0}_msg_qos'.format(sys.argv[1])
    while not zk_lock_store.get_lock_info(lock_task_name):
        time.sleep(60)
    app_log.info('{0} msg qos get lock successfully', sys.argv[1])
    if sys.argv[1] == 'vip':
        msg_qos.start_vip_msg_handler()
        msg_qos.start_vip_redo_msg_handler()
    elif sys.argv[1] == 'experience':
        msg_qos.start_experience_msg_handler()
        msg_qos.start_experience_redo_msg_handler()
        msg_qos.start_check_msg_num()
        msg_qos.start_check_final_msg_num()
    while True:
        time.sleep(60)
