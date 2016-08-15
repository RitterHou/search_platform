# coding=utf-8

"""
ES路由模块
"""
import random

from common.admin_config import admin_config

from common.configs import config
from common.loggers import app_log
from common.utils import get_dict_value_by_path, bind_variable, merge


__author__ = 'liuzhaoming'


class DestinationRouter(object):
    def route(self, destination_cfg, input_param=None):
        """
        获取要路由的目标信息
        :param destination_cfg:
        :param input_param:
        :return:
        """
        raise NotImplementedError

    def get_host(self, destination_cfg, input_param=None):
        """
        获取要路由的 host节点
        :param destination_cfg:
        :param input_param:
        :return:
        """
        raise NotImplementedError

    def get_es_doc_keys(self, destination_cfg, input_param=None, doc_id=None):
        """
        获取要路由的ES文档相关信息
        :param destination_cfg:
        :param input_param:
        :param doc_id:
        :return: index,type,doc_id
        """
        raise NotImplementedError


class RandomDestinationRouter(DestinationRouter):
    """
    随机路由
    """

    def route(self, destination_cfg, input_param=None):
        if 'target' not in destination_cfg['router'] or not len(destination_cfg['router']['target']):
            app_log.error('destination_cfg is invalid {0}', destination_cfg)
            return None

        index = random.randint(0, len(destination_cfg['router']['target']))
        return destination_cfg['router']['target'][index]

    def get_host(self, destination_cfg, input_param=None):
        destination_target = self.route(destination_cfg, input_param)
        if not destination_target or 'host' not in destination_target:
            app_log.error('host is not in destination target cfg {0}', destination_cfg)
            return ''

        host_str = destination_target['host']
        variable_values = config.get_value('consts/custom_variables')
        if host_str and variable_values:
            return host_str.format(**variable_values)
        return host_str

    def get_es_doc_keys(self, destination_cfg, input_param=None, doc_id=None):
        destination_target = self.route(destination_cfg, input_param)
        if not destination_target:
            app_log.error(' destination target cfg is invalid {0}', destination_cfg)

        index_template = get_dict_value_by_path('index', destination_target)
        type_template = get_dict_value_by_path('type', destination_target)
        id_template = get_dict_value_by_path('id', destination_target)
        version = config.get_value('version')
        params = dict(input_param if input_param else {}, **{'version': version})
        index = bind_variable(index_template, params)
        doc_type = bind_variable(type_template, params)
        doc_id = doc_id if doc_id else bind_variable(id_template, params)
        return index.lower() if index else index, doc_type if doc_type else doc_type, doc_id


class VipDestinationRouter(RandomDestinationRouter):
    """
    Vip路由
    """

    # def __init__(self):
    # self.__redis_host = SERVICE_BASE_CONFIG.get('redis')
    # pool = redis.ConnectionPool.from_url(self.__redis_host)
    #     self.__redis_conn = redis.Redis(connection_pool=pool)
    #     self.__vip_users_key = config.get_value(
    #         '/consts/global/vip_router/admin_id_key') or 'search_platform_vip_admin_id_set'
    #     self.__write_lock = False
    #     self.__vip_admin_id_dict = {}
    #     # message_bus.add_event_listener(Event.TYPE_VIP_ADMIN_ID_UPDATE, self.refresh_admin_ids)

    def route(self, destination_cfg, input_param=None):
        if 'target' not in destination_cfg['router'] or not len(destination_cfg['router']['target']):
            app_log.error('destination_cfg is invalid {0}', destination_cfg)
            return None

        if 'fields' not in destination_cfg:
            # 没有配置fields字段，默认为adminId
            admin_id = input_param['adminId'] if 'adminId' in input_param else input_param['adminID']
        else:
            admin_id_template = destination_cfg['router']['fields']['adminId']
            admin_id = bind_variable(admin_id_template, input_param)

        is_vip_flag = admin_config.is_vip(admin_id)
        return self._get_target(destination_cfg, is_vip_flag)

    def _get_target(self, destination_cfg, is_vip_flag):
        """
        获取VIP用户target配置
        :param destination_cfg
        :param is_vip_flag
        :return:
        """

        def is_vip_target(target):
            """
            是否是VIP路由目标
            :param target:
            :return:
            """
            return True if 'tag' in target and target['tag'] == 'vip' else False

        def is_default_target(target):
            """
            是否是默认target
            :param target:
            :return:
            """
            if 'tag' in target:
                return target['tag'] == 'default'

            return True

        if is_vip_flag:
            filter_target_list = filter(is_vip_target, destination_cfg['router']['target'])
        else:
            filter_target_list = filter(is_default_target, destination_cfg['router']['target'])

        return filter_target_list[0] if filter_target_list else None


class CompatibilityDestinationRouter(RandomDestinationRouter):
    """
    兼容旧的配置
    """

    def route(self, destination_cfg, input_param=None):
        return destination_cfg


random_router = RandomDestinationRouter()
vip_router = VipDestinationRouter()
compatibility_router = CompatibilityDestinationRouter()


class RouterProxy(DestinationRouter):
    """
    Router Proxy
    """

    def __init__(self):
        self.__router_dict = {'vip_router': vip_router, 'random_router': random_router, 'default': random_router,
                              'compatibility_router': compatibility_router}

    def route(self, destination_cfg, input_param=None):
        router = self.__get_router(destination_cfg)
        return router.route(destination_cfg, input_param)

    def get_host(self, destination_cfg, input_param=None):
        router = self.__get_router(destination_cfg)
        return router.get_host(destination_cfg, input_param)

    def get_es_doc_keys(self, destination_cfg, input_param=None, doc_id=None):
        router = self.__get_router(destination_cfg)
        return router.get_es_doc_keys(destination_cfg, input_param)

    def merge_es_config(self, destination_config):
        """
        合并引用和实际的配置，以实际的为准
        :param destination_config:
        :return:
        """

        def _merge_basic(_destination_cfg):
            if 'reference' in _destination_cfg:
                _es_config = config.get_value('es_index_setting/' + _destination_cfg['reference'])
                _es_config = merge(_es_config, _destination_cfg)
            else:
                _es_config = _destination_cfg
            return _es_config

        if 'router' in destination_config and 'target' in destination_config['router'] and \
                destination_config['router']['target']:
            es_config = merge({'router': {}}, destination_config)
            es_config['router']['target'] = map(_merge_basic, es_config['router']['target'])

            return es_config
        else:
            return _merge_basic(destination_config)

    def __get_router(self, destination_cfg):
        """
        获取对应的实体router处理
        :param destination_cfg:
        :return:
        """
        if 'router' not in destination_cfg:
            return self.__router_dict['compatibility_router']

        router_type = destination_cfg['router']['type'] if 'type' in destination_cfg['router'] else 'default'

        if router_type not in self.__router_dict:
            app_log.error("not support router type {0}, destination_cfg={1}", router_type, destination_cfg)
            router_type = 'default'

        return self.__router_dict[router_type]


es_router = RouterProxy()