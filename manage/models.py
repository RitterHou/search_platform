# coding=utf-8
import xmlrpclib
from collections import OrderedDict

from common.configs import config
from common.exceptions import UpdateDataNotExistError
from common.loggers import app_log
from common.adapter import es_adapter


__author__ = 'liuzhaoming'


class DataRiver(object):
    """
    数据流
    """

    def get(self, river_name=None):
        """
        获取数据流，如果指定了名称，则返回给定名称的数据流
        :param river_name:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        cfg_rivers = data_river_cfg['data_river'].get('rivers')
        if not cfg_rivers:
            return []
        if river_name:
            return filter(lambda river: river.get('name') == river_name, cfg_rivers)
        return cfg_rivers

    def save(self, data_river):
        """
        新增加数据流
        :param data_river:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        cfg_rivers.append(data_river)
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})

    def update(self, data_river):
        """
        更新数据流，如果同名的数据流不存在，则返回错误
        :param data_river:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        filter_river_list = filter(lambda (index, river): river.get('name') == data_river.get('name'),
                                   enumerate(cfg_rivers))
        if not len(filter_river_list):
            raise UpdateDataNotExistError()
        cfg_rivers[filter_river_list[0][0]] = data_river
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})

    def delete(self, name):
        """
        删除数据流
        :param name:
        :return:
        """
        data_river_cfg = es_adapter.get_config('data_river')
        if 'rivers' not in data_river_cfg['data_river']:
            data_river_cfg['data_river']['rivers'] = []
        cfg_rivers = data_river_cfg['data_river']['rivers']
        filter_river_list = filter(lambda (index, river): river.get('name') == name, enumerate(cfg_rivers))
        if not len(filter_river_list):
            app_log.info('delete data river not exist {0}', name)
            return

        del cfg_rivers[filter_river_list[0][0]]
        es_adapter.save_config({'data_river': {'rivers': cfg_rivers}})


class EsTmpl(object):
    """
    ES模板
    """

    def get(self, tmpl_name=None):
        """
        获取ES模板，如果指定了名称，则返回给定名称的ES模板
        :param tmpl_name:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        if not tmpl_cfg:
            app_log.info("Get Estmpl cannot find es_index_setting")
            return []
        tmpl_list = map(lambda (key, value): OrderedDict(value, **{'name': key}), tmpl_cfg.iteritems())
        if tmpl_name:
            return filter(lambda item: item.get('name') == tmpl_name, tmpl_list)
        return tmpl_list

    def save(self, tmpl):
        """
        新增加ES模板
        :param tmpl:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        if not tmpl or not tmpl.get('name'):
            app_log.info("Save Estmpl input param is invalid {0}", tmpl)
            return
        if tmpl.get('name') in tmpl_cfg:
            app_log.info("Save Estmpl tmpl name is exist {0}", tmpl.get('name'))
            return

        tmpl_cfg[tmpl.get('name')] = tmpl
        del tmpl['name']
        es_adapter.save_config({'es_index_setting': tmpl_cfg})

    def update(self, tmpl):
        """
        更新ES模板，如果同名的ES模板不存在，则返回错误
        :param data_river:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        if not tmpl or not tmpl.get('name'):
            app_log.info("Update Estmpl input param is invalid {0}", tmpl)
            return
        if tmpl.get('name') not in tmpl_cfg:
            app_log.info("Update Estmpl tmpl name is not exist {0}", tmpl.get('name'))
            return

        tmpl_cfg[tmpl.get('name')] = tmpl
        del tmpl['name']
        es_adapter.save_config({'es_index_setting': tmpl_cfg})

    def delete(self, name):
        """
        删除ES模板
        :param name:
        :return:
        """
        tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
        if name not in tmpl_cfg:
            app_log.info("Update Estmpl tmpl name is not exist {0}", name)
            return

        del tmpl_cfg[name]
        es_adapter.save_config({'es_index_setting': tmpl_cfg})


class QueryChain(object):
    """
    HTTP处理链
    """

    def get(self, handler_name=None):
        """
        获取REST处理器，如果指定了名称，则返回给定名称的REST处理器
        :param handler_name:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        cfg_handlers = query_chain_cfg['query'].get('chain')
        if not cfg_handlers:
            app_log.info("Get QueryChain cannot find query")
            return []
        if handler_name:
            return filter(lambda handler: handler.get('name') == handler_name, cfg_handlers)
        return cfg_handlers

    def save(self, handler):
        """
        新增加REST处理器
        :param data_river:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        cfg_handlers.append(handler)
        es_adapter.save_config({'query': {'chain': cfg_handlers}})

    def update(self, handler):
        """
        更新REST处理器，如果同名的REST处理器不存在，则返回错误
        :param data_river:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        filter_handler_list = filter(lambda (index, river): river.get('name') == handler.get('name'),
                                     enumerate(cfg_handlers))
        if not len(filter_handler_list):
            raise UpdateDataNotExistError()
        cfg_handlers[filter_handler_list[0][0]] = handler
        es_adapter.save_config({'query': {'chain': cfg_handlers}})

    def delete(self, name):
        """
        删除REST处理器
        :param name:
        :return:
        """
        query_chain_cfg = es_adapter.get_config('query')
        if 'chain' not in query_chain_cfg['query']:
            query_chain_cfg['query']['chain'] = []
        cfg_handlers = query_chain_cfg['query']['chain']
        filter_handler_list = filter(lambda (index, river): river.get('name') == name,
                                     enumerate(cfg_handlers))
        if not len(filter_handler_list):
            app_log.info('delete query chain not exist {0}', name)
            return

        del cfg_handlers[filter_handler_list[0][0]]
        es_adapter.save_config({'query': {'chain': cfg_handlers}})


class SystemParam(object):
    """
    系统配置参数
    """

    def get(self):
        """
        获取系统参数
        :return:
        """
        sys_param_cfg = es_adapter.get_config('consts').get('consts')
        if not sys_param_cfg:
            app_log.info("Get System Param cannot find consts")
            return {}
        return sys_param_cfg

    def save(self, sys_params):
        """
        更新系统参数
        :param sys_params:
        :return:
        """
        if not sys_params:
            app_log.info("Get System Param input sys param is None")
            return
        # system_param_cfg = es_adapter.get_config('consts')
        # system_param_cfg['consts'] = sys_params
        es_adapter.save_config({'consts': sys_params})


class Supervisor(object):
    def get_cluster_supervisor_info(self, host_addr=None):
        """
        获取搜索平台supervisor信息
        :param host_addr:
        :return:
        """
        hosts = self.__get_hosts(host_addr)
        return filter(lambda item: item, map(self.get_supervisor_info, hosts))

    def get_supervisor_info(self, host_info):
        """
        获取单个服务器上supervisor相关信息
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        try:
            supervisor_info = self.get_supervisor_process_info(host_info)
            supervisor_info['sub_process_list'] = self.get_supervisor_sub_process_list(host_info)
            return supervisor_info
        except Exception as e:
            app_log.error('get_supervisor_info has error {0}', e, host_info)

    def get_supervisor_process_info(self, host_info):
        """
        获取supervisor进程信息
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        supervisor_proxy = self.__get_proxy(host_info=host_info)
        state = supervisor_proxy.getState()
        pid = supervisor_proxy.getPID()
        return {'state': state['statename'], 'pid': pid, 'host': host_info['host']}

    def get_supervisor_sub_process_list(self, host_info):
        """
        获取supervisor子进程集合
        :param host_info:
        :return:
        """
        if not host_info:
            return {}
        supervisor_proxy = self.__get_proxy(host_info=host_info)
        process_info_list = supervisor_proxy.getAllProcessInfo()
        return process_info_list

    def do_action(self, host, action, process_name=None):
        try:
            app_log.info('do_action is called host={0} , action={1}', host, action)
            host_info_list = self.__get_hosts(host)
            if not host_info_list:
                app_log.info('cannot find proxy {0}', host)
                return None
            if action == 'start':
                return self.__do_process_start(host_info_list, process_name)
            elif action == 'stop':
                return self.__do_process_stop(host_info_list, process_name)
            elif action == 'restart':
                return self.__do_process_restart(host_info_list, process_name)
            elif action == 'clear_log':
                return self.__do_process_log_clear(host_info_list, process_name)
            elif action == 'get_log':
                return self.__get_process_log(host_info_list, process_name)
        except Exception as e:
            app_log.exception(e)
            return None


    def __do_process_start(self, host_list, process_name=None):
        """
        启动进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.startProcessGroup(process_name)
                else:
                    supervisor_proxy.startAllProcesses()
            except Exception as e:
                app_log.exception(e)

    def __do_process_stop(self, host_list, process_name=None):
        """
        停止进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.stopProcessGroup(process_name)
                else:
                    supervisor_proxy.stopAllProcesses()
            except Exception as e:
                app_log.exception(e)

    def __do_process_restart(self, host_list, process_name=None):
        """
        重启进程
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.stopProcessGroup(process_name)
                    supervisor_proxy.startProcessGroup(process_name)
                else:
                    supervisor_proxy.stopAllProcesses()
                    supervisor_proxy.startAllProcesses()
            except Exception as e:
                app_log.exception(e)

    def __do_process_log_clear(self, host_list, process_name=None):
        """
        清除进程日志
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    supervisor_proxy.clearProcessLogs(process_name)
                else:
                    supervisor_proxy.clearAllProcessLogs()
            except Exception as e:
                app_log.exception(e)

    def __get_process_log(self, host_list, process_name=None):
        """
        清除进程日志
        :param host_list:
        :param process_name:
        :return:
        """
        for host_info in host_list:
            try:
                supervisor_proxy = self.__get_proxy(host_info=host_info)
                if process_name:
                    return supervisor_proxy.readProcessLog(process_name, 0, 0)
                else:
                    return supervisor_proxy.readMainLog(0, 0)
            except Exception as e:
                app_log.exception(e)

    def __get_hosts(self, host_addr=None):
        """
        根据给定的host地址获取host信息，如果制定的host地址为空，则返回所有的host信息
        :param host_addr:
        :return:
        """
        hosts = self.__get_all_hosts()
        if host_addr:
            hosts = filter(lambda host: host['host'] == host_addr, hosts)
        return hosts


    def __get_proxy(self, host_url=None, host_info=None):
        """
        获取XML-RPC代理
        :param host_url:
        :param host_info:
        :return:
        """
        if host_url:
            __host_url = host_url
        elif host_info:
            __host_url = 'http://' + host_info['host'] + ':' + host_info['supervisor_port'] + '/RPC2'
        if __host_url not in SUPERVISOR_PROXY_CACHE:
            server = xmlrpclib.ServerProxy(__host_url)
            proxy = server.supervisor
            SUPERVISOR_PROXY_CACHE[__host_url] = proxy
        return SUPERVISOR_PROXY_CACHE[__host_url]

    def __get_all_hosts(self):
        """
        获取搜索引擎所有的主机信息
        :return:
        """
        host_list = config.get_value('consts/manager/hosts')
        return host_list


SUPERVISOR_PROXY_CACHE = {}

supervisor = Supervisor()
data_river = DataRiver()
query_chain = QueryChain()
sys_param = SystemParam()
es_tmpl = EsTmpl()

if __name__ == '__main__':
    import json


    # proxy = xmlrpclib.ServerProxy('http://localhost:9001/RPC2')
    # supervisor = proxy.supervisor
    # if not supervisor:
    # print supervisor.getState()
    # print supervisor.getPID()
    # print supervisor.getAllProcessInfo()
    # print supervisor.readLog(-300, 0)
    f = open('../tmp/config.json')
    config_data = json.load(f, object_pairs_hook=OrderedDict)
    es_adapter.insert_config(config_data['default'])
    print es_adapter.get_config()
