# coding=utf-8
"""
SLA等级服务策略
"""
import threading
import time
import ujson as json
from datetime import date
from itertools import groupby
from multiprocessing.dummy import Pool

import jsonpickle
from common.admin_config import admin_config
from common.configs import config
from common.connections import RedisConnectionFactory, KafkaClientFactory
from common.exceptions import MsgHandlingFailError, RedoMsgQueueFullError, FinalFailMsgQueueFullError, MsgQueueFullError
from common.loggers import app_log
from common.rest_quest import RestRequest
from search_platform.settings import SERVICE_BASE_CONFIG

__author__ = 'liuzhaoming'


class MsgSLA(object):
    """
    MQ消息SLA服务等级
    """

    def __init__(self, ):
        self.msg_process_cache = {}
        self.final_msg_alarm_status = []
        # 需要检查消息队列消息数目的vip用户
        self.need_check_msg_num_vip_admin_ids = {}
        # 需要检查消息队列消息数目的体验用户
        self.need_check_msg_num_experience_admin_ids = {}

        # vip消息最大处理次数
        self._vip_sla_max_calls = config.get_value('/consts/global/admin_id_cfg/vip_max_msg') or 5
        # vip用户消息次数计算周期
        self._vip_sla_time_interval = config.get_value('/consts/global/admin_id_cfg/vip_time_interval') or 5
        # 体验用户消息最大处理次数
        self._experience_sla_max_calls = config.get_value('/consts/global/admin_id_cfg/experience_max_msg') or 1
        # 体验用户消息次数计算周期
        self._experience_sla_time_interval = config.get_value(
            '/consts/global/admin_id_cfg/experience_time_interval') or 5
        # vip用户失败消息是否重做
        self._vip_msg_redo_enable = config.get_value('/consts/global/admin_id_cfg/vip_msg_redo_enable') or True
        # 体验用户失败消息是否重做
        self._experience_msg_redo_enable = config.get_value(
            '/consts/global/admin_id_cfg/experience_msg_redo_enable') or False
        # vip用户失败消息重做策略
        self._vip_fail_msg_policy = config.get_value('/consts/global/admin_id_cfg/vip_fail_msg_policy') or {}
        # 体验用户失败消息重做策略
        self._experience_fail_msg_policy = config.get_value(
            '/consts/global/admin_id_cfg/experience_fail_msg_policy') or {}
        # 最终失败消息队列key
        self._final_msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_final_queue_key') or "sp_msg_final_queue"
        # 重做消息队列key
        self._redo_msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_redo_queue_key') or "sp_msg_redo_queue_{0}"
        # 有重做消息的用户admin ID队列key
        self._redo_msg_admin_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_redo_admin_queue_key') or "sp_msg_redo_admin_queue"
        # 消息队列key
        self._msg_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_queue_key') or "sp_msg_queue_{0}"
        self._msg_queue_key_prefix = config.get_value(
            '/consts/global/admin_id_cfg/msg_queue_key_prefix') or "sp_msg_queue_*"
        self._msg_queue_key_pos = len(self._msg_queue_key_prefix) - 1
        # 有消息的用户admin ID队列key
        self._msg_admin_queue_key = config.get_value(
            '/consts/global/admin_id_cfg/msg_admin_queue_key') or "sp_msg_admin_queue"
        # vip用户每次重做消息数目
        self._vip_redo_msg_iter_size = config.get_value(
            '/consts/global/admin_id_cfg/vip_redo_iter_capacity') or 50
        # 体验用户每次重做消息数目
        self._experience_redo_msg_iter_size = config.get_value(
            '/consts/global/admin_id_cfg/experience_redo_iter_capacity') or 5
        # vip用户重做消息线程数目
        self._vip_redo_thread_num = config.get_value(
            '/consts/global/admin_id_cfg/vip_redo_thread_num') or 5
        # 体验用户重做消息线程数目
        self._experience_redo_thread_num = config.get_value(
            '/consts/global/admin_id_cfg/experience_redo_thread_num') or 2

        # vip用户每次处理正常消息数目
        self._vip_msg_iter_size = config.get_value(
            '/consts/global/admin_id_cfg/vip_msg_iter_capacity') or 5
        # 体验用户每次处理正常消息数目
        self._experience_msg_iter_size = config.get_value(
            '/consts/global/admin_id_cfg/experience_msg_iter_capacity') or 1
        # vip用户处理正常消息线程数目
        self._vip_msg_thread_num = config.get_value(
            '/consts/global/admin_id_cfg/vip_msg_thread_num') or 5
        # 体验用户处理正常消息线程数目
        self._experience_msg_thread_num = config.get_value(
            '/consts/global/admin_id_cfg/experience_msg_thread_num') or 2

        # vip用户重做消息队列告警阈值
        self._vip_redo_queue_threshold = config.get_value(
            '/consts/global/admin_id_cfg/vip_redo_queue_threshold') or 200
        # 体验用户重做消息队列告警阈值
        self._experience_redo_queue_threshold = config.get_value(
            '/consts/global/admin_id_cfg/experience_redo_queue_threshold') or 100
        # vip用户消息队列告警阈值
        self._vip_msg_queue_threshold = config.get_value(
            '/consts/global/admin_id_cfg/vip_msg_queue_threshold') or 200
        # 体验用户消息队列告警阈值
        self._experience_msg_queue_threshold = config.get_value(
            '/consts/global/admin_id_cfg/experience_msg_queue_threshold') or 100
        # 最终失败消息队列告警阈值
        self._final_queue_threshold = config.get_value(
            '/consts/global/admin_id_cfg/final_queue_threshold') or 500

        self._redis_host = SERVICE_BASE_CONFIG.get('msg_queue')
        self._redis_conn = RedisConnectionFactory.get_redis_connection(self._redis_host)
        self._vip_redo_pool = Pool(self._vip_redo_thread_num)
        self._experience_redo_pool = Pool(self._experience_redo_thread_num)
        self._vip_msg_pool = Pool(self._vip_msg_thread_num)
        self._experience_msg_pool = Pool(self._experience_msg_thread_num)

    def limit_rate(self, fun, module='mq_msg', max_calls=None, time_interval=None):
        """
        速度控制
        :param module:
        :param fun:
        :param max_calls:
        :param time_interval:
        :return:
        """
        pass

    def _get_msg_cache(self, admin_id):
        """
        获取状态控制信息统计
        :param admin_id:
        :return:
        """
        if admin_id not in self.msg_process_cache:
            self.msg_process_cache[admin_id] = {}

        return self.msg_process_cache[admin_id]

    def limit_msg_rate(self, admin_id, fun, max_calls=None, time_interval=None, *args, **kwargs):
        """
        现在MQ消息消费的速度
        :param admin_id:
        :param fun:
        :param max_calls:
        :param time_interval:
        :param args:
        :param kwargs:
        :return:
        """
        if not max_calls:
            max_calls = admin_config.get_admin_param_value(admin_id, 'max_msg') or (
                self._vip_sla_max_calls if admin_config.is_vip(admin_id) else self._experience_sla_max_calls)

        if not time_interval:
            time_interval = admin_config.get_admin_param_value(admin_id, 'msg_time_interval') or (
                self._vip_sla_time_interval if admin_config.is_vip(admin_id) else self._experience_sla_time_interval)

        msg_status_cache = self._get_msg_cache('mq_msg', admin_id)
        cur_time = time.time()
        if 'last_time' not in msg_status_cache:
            msg_status_cache['last_time'] = cur_time
        if 'last_calls' not in msg_status_cache:
            msg_status_cache['last_calls'] = 0

        time_delta = cur_time - msg_status_cache['last_time']
        if msg_status_cache['last_calls'] > max_calls:
            if time_delta < time_interval:
                if 'print_log' not in msg_status_cache or not msg_status_cache['print_log']:
                    app_log.warning("The admin id {} msg exceed limit {} {}", admin_id, max_calls, time_interval)
                    msg_status_cache['print_log'] = True

                return None

        if time_delta >= time_interval:
            msg_status_cache['last_calls'] = 1
            msg_status_cache['last_time'] = cur_time
        else:
            msg_status_cache['last_calls'] += 1

        return fun(*args, **kwargs)

    def send_msg_to_queue(self, *msgs):
        """
        将消息发送到消息队列
        :param msgs:
        :return:
        """
        if not msgs:
            return

        app_log.info("Send msg to queue {}", msgs)
        admin_id_list = []
        for admin_id, admin_msgs in groupby(msgs, lambda msg: msg['adminId'] if 'adminId' in msg else 'default'):
            admin_id_list.append(admin_id)
            str_admin_msgs = map(lambda msg: json.dumps(msg), admin_msgs)
            str_key = self._msg_queue_key.format(admin_id)
            try:
                self._redis_conn.rpush(str_key, *str_admin_msgs)
            except Exception as e:
                app_log.error('Send msg to queue fail, admin_id={0}, msgs={1}', e, admin_id, str_admin_msgs)
        try:
            self._redis_conn.sadd(self._msg_admin_queue_key, *admin_id_list)
        except Exception as e:
            app_log.error('Send msg admin id to queue fail, admin_ids={0}', e, admin_id_list)

    def process_msg(self, msg_handler_fun, is_vip=True):
        """
        启动消息处理
        :param msg_handler_fun:
        :param is_vip
        :return:
        """
        experience_admin_ids, vip_admin_ids = self._query_msg_admin_ids()
        # app_log.info('kkkkkk process_msg experience ids={0},  is_vip={1}', experience_admin_ids, is_vip)
        if is_vip:
            if vip_admin_ids:
                self._vip_msg_pool.map(lambda vip_admin_id: self.process_admin_msg(vip_admin_id, msg_handler_fun),
                                       vip_admin_ids)
        else:
            if experience_admin_ids:
                self._experience_msg_pool.map(
                    lambda experience_admin_id: self.process_admin_msg(experience_admin_id, msg_handler_fun),
                    experience_admin_ids)

    def process_admin_msg(self, admin_id, msg_handler_fun):
        """
        消息处理
        :param admin_id:
        :return:
        """
        try:
            cur_msgs = self._fetch_msg(admin_id)
            if not cur_msgs:
                return

            app_log.info("sla fetch messages {0}".format(cur_msgs))
            msg_handler_fun(cur_msgs)

            self._update_msg_process_status(admin_id, cur_msgs)

            self._remove_msg(admin_id, len(cur_msgs))
        except Exception as e:
            app_log.error("process admin {0} msg fail ".format(admin_id))
            app_log.exception(e)

    def process_redo_msg(self, msg_handler_fun, is_vip=True):
        """
        启动消息重做任务
        :param msg_handler_fun
        :param is_vip
        :return:
        """
        experience_admin_ids, vip_admin_ids = self._query_redo_admin_ids()
        if is_vip:
            if self._vip_msg_redo_enable and vip_admin_ids:
                self._vip_redo_pool.map(lambda vip_admin_id: self.process_admin_redo_msg(vip_admin_id, msg_handler_fun),
                                        vip_admin_ids)
        else:
            if self._experience_msg_redo_enable and experience_admin_ids:
                self._experience_redo_pool.map(
                    lambda experience_admin_id: self.process_admin_redo_msg(experience_admin_id, msg_handler_fun),
                    experience_admin_ids)

        # 检查最终失败消息队列是否超过阈值
        self.check_final_msg_num()

    def process_admin_redo_msg(self, admin_id, msg_handler_fun):
        """
        消息重做
        :param admin_id:
        :return:
        """
        cur_redo_msgs = self._fetch_redo_msg(admin_id)
        if not cur_redo_msgs:
            return

        cur_time = time.time()
        not_to_time_msgs = []
        need_redo_msgs = []
        final_msgs = []

        try:
            for msg in cur_redo_msgs:
                self._do_redo_msg(msg, msg_handler_fun, cur_time, not_to_time_msgs, need_redo_msgs, final_msgs)

            if not_to_time_msgs:
                app_log.info('begin to send not to time msgs to redo queue')
                self._send_msg_to_redo_queue_by_admin(not_to_time_msgs, admin_id)

            if need_redo_msgs:
                app_log.info('begin to send redo fail msgs to redo queue')
                self._send_msg_to_redo_queue_by_admin(not_to_time_msgs, admin_id)

            if final_msgs:
                app_log.info('begin to send redo fail msgs to final queue')
                self._send_msg_to_final_queue(final_msgs)
        finally:
            self._remove_redo_msg(admin_id, len(cur_redo_msgs))

    def _query_msg_admin_ids_by_set(self):
        """
        查询需要处理消息的Admin用户ID，已经废弃，现在直接通过keys * 来查询adminId
        :return:
        """
        admin_ids = self._redis_conn.smembers(self._msg_admin_queue_key)
        vip_admin_ids = []
        experience_admin_ids = []
        for admin_id in admin_ids:
            if admin_config.is_vip(admin_id):
                vip_admin_ids.append(admin_id)
            else:
                experience_admin_ids.append(admin_id)
        return experience_admin_ids, vip_admin_ids

    def _query_msg_admin_ids(self):
        """
        查询需要处理消息的Admin用户ID，通过keys * 来查询adminId
        :return:
        """
        admin_msg_keys = self._redis_conn.keys(self._msg_queue_key_prefix)
        admin_ids = map(lambda msg_key: msg_key[self._msg_queue_key_pos:], admin_msg_keys)
        vip_admin_ids = []
        experience_admin_ids = []
        for admin_id in admin_ids:
            if admin_config.is_vip(admin_id):
                vip_admin_ids.append(admin_id)
            else:
                experience_admin_ids.append(admin_id)
        return experience_admin_ids, vip_admin_ids

    def _query_redo_admin_ids(self):
        """
        查询需要消息重做的Admin用户ID
        :return:
        """
        admin_ids = self._redis_conn.smembers(self._redo_msg_admin_queue_key)
        vip_admin_ids = []
        experience_admin_ids = []
        for admin_id in admin_ids:
            if admin_config.is_vip(admin_id):
                vip_admin_ids.append(admin_id)
            else:
                experience_admin_ids.append(admin_id)
        return experience_admin_ids, vip_admin_ids

    def _update_msg_process_status(self, admin_id, cur_msgs):
        """
        更新用户消息队列处理状态
        :param admin_id:
        :param cur_msgs:
        :return:
        """
        msg_status_cache = self._get_msg_cache(admin_id)
        msg_status_cache['last_calls'] += len(cur_msgs)

    def _fetch_msg(self, admin_id):
        """
        获取用户队列的正常消息
        :param admin_id:
        :return:
        """

        def __convert_msg(_msg_str):
            try:
                return json.loads(_msg_str)
            except Exception as e:
                app_log.error("Admin {0} has invalid message {1}".format(admin_id, _msg_str))
                return None

        try:
            str_key = self._msg_queue_key.format(admin_id)
            iter_size, is_vip = self._get_msg_iter_size(admin_id)
            if iter_size == 0:
                return []

            str_msgs = self._redis_conn.lrange(str_key, 0, iter_size - 1)

            self._set_need_check_msg_num(admin_id, is_vip, iter_size, str_msgs)

            return filter(lambda _: _, map(__convert_msg, str_msgs))
        except Exception as e:
            app_log.error('fetch msg error, admin_id={0}', e, admin_id)

    def _set_need_check_msg_num(self, admin_id, is_vip, iter_size, str_msgs):
        """
        设置该admin ID是否需要检查消息数目
        :param admin_id:
        :param is_vip:
        :param iter_size:
        :param str_msgs:
        :return:
        """
        if is_vip:
            is_contain = admin_id in self.need_check_msg_num_vip_admin_ids
            if len(str_msgs) == iter_size:
                if not is_contain:
                    self.need_check_msg_num_vip_admin_ids[admin_id] = ''
            elif is_contain:
                del self.need_check_msg_num_vip_admin_ids[admin_id]
        else:
            is_contain = admin_id in self.need_check_msg_num_experience_admin_ids
            if len(str_msgs) == iter_size:
                if not is_contain:
                    self.need_check_msg_num_experience_admin_ids[admin_id] = ''
            elif is_contain:
                del self.need_check_msg_num_experience_admin_ids[admin_id]

    def _fetch_redo_msg(self, admin_id):
        """
        从用户队列中获取指定数目的redo消息
        :param admin_id:
        :return:
        """
        try:
            str_key = self._redo_msg_queue_key.format(admin_id)
            iter_size = self._get_redo_iter_size(admin_id)
            str_redo_msgs = self._redis_conn.lrange(str_key, 0, iter_size - 1)

            if len(str_redo_msgs) == iter_size:
                self._check_admin_redo_msg_num(admin_id, str_key)

            return map(lambda msg_str: json.loads(msg_str), str_redo_msgs)
        except Exception as e:
            app_log.error('fetch redo msg error, admin_id={0}', e, admin_id)

    def check_msg_num(self):
        """
        检查消息数目阈值
        :return:
        """
        for vip_admin_id in self.need_check_msg_num_vip_admin_ids:
            self._check_admin_msg_num(vip_admin_id, True)

        for experience_admin_id in self.need_check_msg_num_experience_admin_ids:
            self._check_admin_msg_num(experience_admin_id, False)

    def _check_admin_msg_num(self, admin_id, is_vip):
        """
        判断消息队列是否达到阈值
        :param admin_id:
        :param is_vip
        :return:
        """
        try:
            str_key = self._msg_queue_key.format(admin_id)
            queue_size = self._redis_conn.llen(str_key)
            threshold = self._vip_msg_queue_threshold if is_vip else self._experience_msg_queue_threshold
            if queue_size >= threshold:
                app_log.exception(MsgQueueFullError(admin_id, queue_size, is_vip))

        except Exception as e:
            app_log.error('check admin msg num error, admin_id={0}', e, admin_id)

    def _check_admin_redo_msg_num(self, admin_id, str_key):
        """
        判断重做消息队列是否达到阈值
        :param admin_id:
        :return:
        """
        try:
            redo_queue_size = self._redis_conn.llen(str_key)
            is_vip = admin_config.is_vip(admin_id)
            threshold = self._vip_redo_queue_threshold if is_vip else self._experience_redo_queue_threshold
            if redo_queue_size >= threshold:
                app_log.exception(RedoMsgQueueFullError(admin_id, redo_queue_size, is_vip))

        except Exception as e:
            app_log.error('check admin redo msg num error, admin_id={0}', e, admin_id)

    def check_final_msg_num(self):
        """
        判断最终失败消息队列是否达到阈值，因为最终消息队列需要手工处理，因此一天只报警一次
        :return:
        """
        try:
            if len(self.final_msg_alarm_status) > 0:
                if date.today() in self.final_msg_alarm_status:
                    # 表示今天已经告警过了，不需要再次告警
                    return
                else:
                    # 以前的告警，需要清除掉
                    self.final_msg_alarm_status = []

            final_queue_size = self._redis_conn.llen(self._final_msg_queue_key)
            if final_queue_size >= self._final_queue_threshold:
                app_log.exception(FinalFailMsgQueueFullError(final_queue_size))

                self.final_msg_alarm_status.append(date.today())
        except Exception as e:
            app_log.error('check final msg num error', e)

    def _remove_redo_msg(self, admin_id, size):
        """
        从用户队列中删除redo消息
        :param admin_id:
        :param size:
        :return:
        """
        try:
            str_key = self._redo_msg_queue_key.format(admin_id)
            self._redis_conn.ltrim(str_key, size, -1)
        except Exception as e:
            app_log.error('remove redo msg error, admin_id={0}, size={1}', e, admin_id, size)

    def _remove_msg(self, admin_id, size):
        """
        从用户队列中删除消息
        :param admin_id:
        :param size:
        :return:
        """
        try:
            str_key = self._msg_queue_key.format(admin_id)
            self._redis_conn.ltrim(str_key, size, -1)
        except Exception as e:
            app_log.error('remove msg error, admin_id={0}, size={1}', e, admin_id, size)

    def _get_msg_iter_size(self, admin_id):
        """
        获取该用户正常消息批量大小, 取用户每次迭代消息大小 和 本周期下用户还允许消费的消息数目最小值
        :param admin_id:
        :return:
        """
        is_vip = admin_config.is_vip(admin_id)
        config_msg_iter_size = self._vip_msg_iter_size if is_vip else self._experience_msg_iter_size

        max_calls = admin_config.get_admin_param_value(admin_id, 'max_msg') or (
            self._vip_sla_max_calls if is_vip else self._experience_sla_max_calls)

        time_interval = admin_config.get_admin_param_value(admin_id, 'msg_time_interval') or (
            self._vip_sla_time_interval if is_vip else self._experience_sla_time_interval)

        msg_status_cache = self._get_msg_cache(admin_id)
        cur_time = time.time()
        if 'last_time' not in msg_status_cache:
            msg_status_cache['last_time'] = cur_time
        if 'last_calls' not in msg_status_cache:
            msg_status_cache['last_calls'] = 0

        time_delta = cur_time - msg_status_cache['last_time']
        if msg_status_cache['last_calls'] > max_calls:
            if time_delta < time_interval:
                if 'print_log' not in msg_status_cache or not msg_status_cache['print_log']:
                    app_log.warning("The admin id {} msg exceed limit {} {}", admin_id, max_calls, time_interval)
                    msg_status_cache['print_log'] = True

                return 0, is_vip

        if time_delta >= time_interval:
            msg_status_cache['last_calls'] = 0
            msg_status_cache['last_time'] = cur_time
            msg_status_cache['print_log'] = False

        left_msg_calls = max_calls - msg_status_cache['last_calls']

        return min(config_msg_iter_size, left_msg_calls), is_vip

    def _get_redo_iter_size(self, admin_id):
        """
        获取该用户消息重做的批量大小
        :param admin_id:
        :return:
        """
        return self._vip_redo_msg_iter_size if admin_config.is_vip(admin_id) else self._experience_redo_msg_iter_size

    def _do_redo_msg(self, msg, msg_handler_fun, cur_time, not_to_time_msgs, need_redo_msgs, final_msgs):
        """
        重做消息
        :param msg:
        :param msg_handler_fun:
        :param cur_time:
        :param not_to_time_msgs:
        :param need_redo_msgs:
        :param final_msgs:
        :return:
        """
        count = msg['redo_num'] if 'redo_num' in msg else 0
        period = float(msg['redo_interval'][count])
        if cur_time < msg['time'] + period * 60:
            # 表示未到重做时间，重新放回队列
            not_to_time_msgs.append(msg)
        else:
            msg['redo_time'].append(cur_time)
            msg['redo_num'] += 1
            try:
                msg_handler_fun([msg])
            except Exception as e:
                # 重做消息失败，
                app_log.error("Redo msg error, {0}", e, msg)
                if msg['redo_num'] >= count:
                    # 如果已到重做次数，则放到最终队列中
                    final_msgs.append(msg)
                else:
                    # 如果未到重做次数，则放到重做队列中；
                    need_redo_msgs.append(msg)

    def process_do_error_message(self, msg, exception):
        """
        处理失败的消息
        :param msg:
        :param exception:
        :return:
        """
        msg['error'] = str(exception)
        if 'adminId' not in msg or not msg['adminId'] or not isinstance(exception, MsgHandlingFailError):
            # 如果消息中没有admin ID或者异常不是消息处理异常，则不进行消息重做，直接添加到最终失败消息队列
            return self._send_msg_to_final_queue(msg)

        admin_id = msg['adminId']
        redo_policy = self._get_msg_redo_policy(admin_id)
        if not redo_policy:
            return self._send_msg_to_final_queue(msg)

        need_redo, processed_msg = self._get_msg_redo_info(msg, exception, redo_policy)
        if need_redo:
            self._send_msg_to_redo_queue_by_admin([msg], admin_id)
        else:
            self._send_msg_to_final_queue([msg])

    def _get_msg_redo_info(self, msg, exception, redo_policy):
        """
        获取消息重做相关信息，如果消息需要重做，返回(True, msg)；如果消息不需要重做，则返回(False, msg)
        :param msg:
        :param exception:
        :param redo_policy:
        :return:
        """
        # 首先判断该admin用户是否需要redo消息
        admin_redo_enable = self._vip_msg_redo_enable if admin_config.is_vip(
            msg['adminId']) else self._experience_msg_redo_enable
        if not admin_redo_enable:
            return False, msg

        source_key = ''
        if exception.source == exception.DUBBO_ERROR or exception.source == exception.HTTP_ERROR:
            source_key = 'dubbo'
        elif exception.source == exception.PROCESS_ERROR:
            source_key = 'process'
        elif exception.source == exception.ES_ERROR:
            source_key = 'es_error'

        # 检查是否超过重做次数
        total_redo_time = msg['redo_times'] if 'redo_times' in msg else redo_policy[source_key]['redo_times']
        cur_redo_time = msg['redo_num'] if 'redo_num' in msg else 0
        if cur_redo_time >= total_redo_time:
            return False, msg

        if source_key in redo_policy and 'redo_times' in redo_policy[source_key] \
                and redo_policy[source_key]['redo_times'] and 'redo_interval' in redo_policy[source_key] \
                and redo_policy[source_key]['redo_interval']:
            msg.setdefault('redo_time', [])
            msg.setdefault('redo_num', 0)
            msg.setdefault('redo_times', redo_policy[source_key]['redo_times'])
            msg.setdefault('time', exception.event_time)
            if 'redo_interval' not in msg:
                msg['redo_interval'] = redo_policy[source_key]['redo_interval'].strip().split(',')

            return True, msg

        return False, msg

    def _get_msg_redo_policy(self, admin_id):
        """
        获取该用户的消息重做策略
        :param admin_id:
        :return:
        """
        is_vip = admin_config.is_vip(admin_id)
        return self._vip_fail_msg_policy if is_vip else self._experience_fail_msg_policy

    def _send_msg_to_final_queue(self, msgs):
        """
        将失败消息发送到消息最终失败队列
        :param msgs:
        :return:
        """
        app_log.info("Send msg to final queue {}", msgs)
        str_msgs = None
        try:
            str_msgs = map(lambda msg: json.dumps(msg), msgs)
            self._redis_conn.rpush(self._final_msg_queue_key, *str_msgs)
        except Exception as e:
            app_log.error('Send msg to final queue fail, msgs={0}', e, str_msgs)

    def _send_msg_to_redo_queue(self, msgs):
        """
        将失败消息发送到消息重做队列
        :param msgs:
        :return:
        """
        app_log.info("Send msg to redo queue {}", msgs)
        admin_id_list = []
        for admin_id, admin_msgs in groupby(msgs, lambda msg: msg['adminId']):
            admin_id_list.append(admin_id)
            str_admin_msgs = map(lambda msg: json.dumps(msg), admin_msgs)
            str_key = self._redo_msg_queue_key.format(admin_id)
            try:
                self._redis_conn.rpush(str_key, *str_admin_msgs)
            except Exception as e:
                app_log.error('Send msg to redo queue fail, admin_id={0}, msgs={1}', e, admin_id, str_admin_msgs)
        try:
            self._redis_conn.sadd(self._redo_msg_admin_queue_key, *admin_id_list)
        except Exception as e:
            app_log.error('Send redo msg admin id to queue fail, admin_ids={0}', e, admin_id_list)
            app_log.exception(e)

    def _send_msg_to_redo_queue_by_admin(self, msgs, admin_id):
        """
        将单个用户的重做消息发送到重做队列
        :param msgs:
        :param admin_id:
        :return:
        """
        app_log.info("Send msg to redo queue {}", msgs)
        str_admin_msgs = map(lambda msg: json.dumps(msg), msgs)
        str_key = self._redo_msg_queue_key.format(admin_id)
        try:
            self._redis_conn.rpush(str_key, *str_admin_msgs)
        except Exception as e:
            app_log.error('Send msg to redo queue fail, admin_id={0}, msgs={1}', e, admin_id, str_admin_msgs)

        try:
            self._redis_conn.sadd(self._redo_msg_admin_queue_key, admin_id)
        except Exception as e:
            app_log.error('Send redo msg admin id to queue fail, admin_id={0}', e, admin_id)


msg_sla = MsgSLA()


class RestSLA(object):
    """
    RESTFul 接口SLA
    """

    def __init__(self):
        self._kafka_host = config.get_value('/consts/custom_variables/kafka_host')
        self._rest_quest_topic = config.get_value('/consts/query/sla/rest_request_fail_topic') \
                                 or 'search_platform_fail_rest_request'
        self._redo_consumer_group = config.get_value('/consts/query/sla/rest_request_fail_consumer_redo_group') \
                                    or 'redo_consumer_groups'
        self._sla_enable = config.get_value('/consts/query/sla/enable')
        self._kafka_producer = None
        self._kafka_topic = None
        self._mutex = threading.Lock()

    def process_http_error_request(self, request, exception, timestamp):
        """
        处理操作失败的http请求
        :param request:
        :param exception:
        :param timestamp:
        :return:
        """
        try:
            if not self._sla_enable:
                return
            if request.method != 'POST' and request.method != 'DELETE' and request.method != 'PUT':
                return
            message = {'timestamp': timestamp, 'request': jsonpickle.encode(RestRequest(request)),
                       'exception': str(exception)}
            self._send_fail_rest_request(json.dumps(message))
        except Exception as e:
            app_log.error('send fail rest request to kafka ', e)

    def _send_fail_rest_request(self, message):
        """
        发送处理失败REST请求到kafka
        :param message:
        :return:
        """
        producer = self._get_kafka_producer()
        producer.produce(message)

    def get_kafka_topic(self):
        """
        获取kafka消息topic
        :return:
        """
        if self._kafka_topic:
            return self._kafka_topic
        else:
            client = KafkaClientFactory.get_kafka_client(self._kafka_host)
            self._kafka_topic = client.topics[str(self._rest_quest_topic)]
            return self._kafka_topic

    def _get_kafka_producer(self):
        """
        获取kafka消息生产者，如果没有就初始化一个
        :return:
        """
        if self._kafka_producer:
            return self._kafka_producer
        else:
            if self._mutex.acquire():
                if self._kafka_producer:
                    self._mutex.release()
                    return self._kafka_producer
                try:
                    kafka_topic = self.get_kafka_topic()
                    self._kafka_producer = kafka_topic.get_producer(ack_timeout_ms=1000)
                except Exception as e:
                    app_log.error('get kafka producer fail', e)
                finally:
                    self._mutex.release()
                    return self._kafka_producer


rest_sla = RestSLA()
if __name__ == '__main__':
    pass
