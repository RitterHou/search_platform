# coding=utf-8
"""
和admin ID 相关配置，主要是放在REDIS，可以热加载
"""
from itertools import chain
import time

from common.configs import config
from common.connections import RedisConnectionFactory
from common.msg_bus import message_bus, Event
from search_platform.settings import SERVICE_BASE_CONFIG


__author__ = 'liuzhaoming'


class AdminConfig(object):
    """
    adminId 相关配置
    """

    def __init__(self):
        self.__redis_host = SERVICE_BASE_CONFIG.get('redis_admin_id_config')
        self.__redis_conn = RedisConnectionFactory.get_redis_connection(self.__redis_host)
        self.__vip_users_key = config.get_value(
            '/consts/global/admin_id_cfg/vip_id_key') or 'search_platform_vip_admin_id_set'
        self.__admin_id_params_key = config.get_value(
            '/consts/global/admin_id_cfg/admin_id_params_key') or 'search_platform_admin_id_params'
        self.__write_id_lock = False
        self.__write_param_lock = False
        self.__vip_admin_id_dict = {}
        self.__admin_param_dict = {}
        self.refresh_admin_ids()
        message_bus.add_event_listener(Event.TYPE_VIP_ADMIN_ID_UPDATE, self.refresh_admin_ids)
        message_bus.add_event_listener(Event.TYPE_VIP_ADMIN_PARAMS_UPDATE, self.refresh_admin_params)
        # self.__redis_conn.hvals()

    def refresh_admin_ids(self):
        """
        刷新vip admin ID
        :return:
        """
        vip_admin_ids = self._query_vip_admin_ids()
        if vip_admin_ids is not None:
            self.__write_id_lock = True
            time.sleep(0.005)
            vip_admin_id_dict = {}
            map(lambda admin_id: vip_admin_id_dict.setdefault(admin_id, None), vip_admin_ids)
            self.__vip_admin_id_dict = vip_admin_id_dict
            self.__write_id_lock = False

    def refresh_admin_params(self):
        """
        刷新admin param配置
        :return:
        """
        admin_param_dict = self._query_admin_param_cfg()
        if admin_param_dict is not None:
            self.__write_param_lock = True
            time.sleep(0.005)
            self.__admin_param_dict = admin_param_dict
            self.__write_param_lock = False

    def get_admin_params(self):
        """
        获取admin param配置
        :return:
        """
        if self.__write_param_lock:
            while self.__write_param_lock:
                time.sleep(0.01)

        return self.__admin_param_dict

    def get_admin_param_value(self, admin_id, param_key):
        """
        获取admin param value
        :param admin_id:
        :param param_key:
        :return:
        """
        if not admin_id or not param_key:
            return None

        if self.__write_param_lock:
            while self.__write_param_lock:
                time.sleep(0.01)

        if admin_id not in self.__admin_param_dict or param_key not in self.__admin_param_dict[admin_id]:
            return None

        return self.__admin_param_dict[admin_id][param_key]

    def get_vip_admin_ids(self):
        """
        获取vip admin ID列表
        :return:
        """
        if self.__write_id_lock:
            while self.__write_id_lock:
                time.sleep(0.01)

        return self.__vip_admin_id_dict.keys()

    def is_vip(self, admin_id):
        """
        判断admin ID是否是VIP用户
        :param admin_id:
        :return:
        """
        if not admin_id:
            return False

        if self.__write_id_lock:
            while self.__write_id_lock:
                time.sleep(0.007)

        return admin_id in self.__vip_admin_id_dict

    def _query_vip_admin_ids(self):
        """
        从redis查询vip用户
        :return:
        """
        vip_admin_ids = self.__redis_conn.smembers(self.__vip_users_key)
        return vip_admin_ids

    def _query_admin_param_cfg(self):
        """
        查询admin 参数配置
        :return:
        """
        # hscan返回数据结构为[('baidu', 'www.baidu.com'), ('google', 'www.google.com'), ('sina', 'www.sina.com')]
        param_key_value_list = self.__redis_conn.hscan_iter(self.__admin_id_params_key, count=100)
        id_key_value_list = map(lambda (param_key, param_value): chain(param_key.split('##'), [param_value]),
                                param_key_value_list)
        admin_param_dict = {}
        for (admin_id, param_key, param_value) in id_key_value_list:
            if admin_id not in admin_param_dict:
                admin_param_dict[admin_id] = {}
            admin_param_dict[admin_id][param_key] = param_value

        return admin_param_dict


admin_config = AdminConfig()