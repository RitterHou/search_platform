# -*- coding: utf-8 -*-
from json import load
from collections import OrderedDict
import json

from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent
from watchdog.observers import Observer

from common.connections import EsConnectionFactory
from common.registers import register_center
from search_platform.settings import SERVICE_BASE_CONFIG, BASE_DIR
from common.msg_bus import message_bus, Event
from common.loggers import LOGGER_LEVELS, app_log


__author__ = 'liuzhaoming'


def get_config(key=None):
    """
    从ES读取Meta文件
    :param key:
    :return:
    """
    app_log.info('Get config is called')
    try:
        es_connection = EsConnectionFactory.get_es_connection(host=SERVICE_BASE_CONFIG.get('elasticsearch'))
        es_result = es_connection.search(index=SERVICE_BASE_CONFIG.get('meta_es_index'),
                                         doc_type=SERVICE_BASE_CONFIG.get('meta_es_type'))
        doc_list = es_result['hits'].get('hits')
        if not doc_list:
            return {}
        config_data = {}
        for doc in doc_list:
            if doc['_id'] == 'version':
                field_value = doc['_source']['version']
            else:
                field_value = json.loads(doc['_source']['json_str'], object_pairs_hook=OrderedDict) \
                    if 'json_str' in doc['_source'] else {}

            config_data[doc['_id']] = field_value
        if key:
            return {key: config_data[key]} if key in config_data else {}
        return config_data
    except Exception as e:
        app_log.error('Get config from es has error, ', e)
        return {}


class ConfigFileEventHandler(FileSystemEventHandler):
    """
    配置文件监听句柄
    """

    def __init__(self, config_data):
        FileSystemEventHandler.__init__(self)
        self.config = config_data

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and event.src_path == self.config.get_config_file_path():
            app_log.warning('Config file is modified, it could be a dangerous operation {0}', event)
            # self.config.refresh()

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and event.src_path == self.config.get_config_file_path():
            app_log.warning('Config file is modified, it could be a dangerous operation {0}', event)
            # self.config.refresh()


class ConfigHolder(object):
    """
    配置文件管理器，目前配置文件主要有三份：内存、配置文件、远端ES
    """

    def __init__(self):
        if not self.synchronize_config('es', 'cache', True):
            self.synchronize_config('file', 'cache', True)
        message_bus.add_event_listener(Event.TYPE_SYNCHRONIZED_CONFIG, self.on_synchronized_config_event)
        message_bus.add_event_listener(Event.TYPE_UPDATE_LOG_LEVEL, self.on_log_level_update_event)

    def on_synchronized_config_event(self, event):
        """
        配置同步事件响应接口
        :param event:
        :return:
        """
        app_log.info('Received synchronized config event {0}'.format(event))
        data = event.data()
        if not data:
            self.synchronize_config()
        else:
            source = data['source'] if 'source' in data else 'es'
            destination = data['destination'] if 'destination' in data else 'cache'
            self.synchronize_config(source, destination)

    def on_log_level_update_event(self, event):
        """
        日志级别修改事件
        :param event: data的格式为：{type:level}
        :return:
        """
        app_log.info('Received log level update event {0}', event)
        data = event.data
        if not data:
            app_log.info('The log level update event is invalid')
            return
        else:
            logger_config = config.get_value('consts/logger')
            for logger_type in data:
                if logger_type in logger_config and data[logger_type].upper() in LOGGER_LEVELS:
                    logger_config[logger_type]['level'] = data[logger_type].upper()


    def synchronize_config(self, source='es', destination='cache', is_sys_start=False):
        """
        更新配置文件，指定源和目的地
        """
        app_log.info('Synchronize config is called, source={0} , destination={1}', source, destination)
        is_successful, config_data = self.__fetch_config_data(source)
        if not is_successful or config_data is None:
            app_log.error('Cannot fetch data')
            return False
        app_log.info('Fetch config data : {0}', config_data)
        is_successful = self.__put_config(config_data, destination)
        if not is_successful:
            app_log.error('Put config fail')

        if is_successful and destination == 'cache' and not is_sys_start:
            self.notify_config_update_event()
        return is_successful

    def notify_config_update_event(self):
        """
        通知配置数据已经被修改，只有缓存中的数据被更改时才会发出通知
        """
        message_bus.dispatch_event(type=Event.TYPE_CONFIG_UPDATE)

    def __fetch_config_data(self, source):
        if source == 'es':
            return self.__fetch_config_data_from_es()
        elif source == 'file':
            return self.__fetch_config_data_from_file()
        elif source == 'cache':
            return True, config.get_config()
        return False, None

    def __fetch_config_data_from_es(self):
        """
        从ES获取配置数据
        """
        try:
            default_data = get_config()
            return (True, {'default': default_data}) if default_data else (False, None)
        except Exception as e:
            app_log.exception(e)
            return False, None

    def __fetch_config_data_from_file(self):
        """
        从配置文件中获取配置数据
        """
        try:
            meta_file = BASE_DIR + SERVICE_BASE_CONFIG['meta_file']
            f = open(meta_file, 'r')
            meta_data = load(f, 'utf8')
            f.close()
            return True, meta_data
        except Exception as e:
            app_log.exception(e)
            return False, None

    def __put_config_to_file(self, config_data):
        """
        将配置数据存放到文件中
        """
        try:
            meta_file = SERVICE_BASE_CONFIG['meta_file']
            f = open(meta_file, 'w')
            f.write(config_data)
            f.flush()
            f.close()
            return True
        except Exception as e:
            app_log.exception(e)
            return False

    def __put_config_to_es(self, config_data):
        """
        将配置文件存放到ES中
        """
        try:
            es_host = SERVICE_BASE_CONFIG['elasticsearch']
            es_index = SERVICE_BASE_CONFIG['meta_es_index']
            es_type = SERVICE_BASE_CONFIG['meta_es_type']
            es_id = SERVICE_BASE_CONFIG['meta_es_id']
            es_connection = EsConnectionFactory.get_es_connection(es_host)
            if es_connection.exists(es_index, es_id, es_type):
                es_connection.delete(es_index, es_type, es_id)
            return es_connection.index(es_index, es_type, config_data, es_id)
        except Exception as e:
            app_log.exception(e)
            return False

    def __put_config(self, config_data, destination):
        """
        将配置文件写入到目的地
        """
        if destination == 'es':
            return self.__put_config_to_es(config_data)
        elif destination == 'file':
            return self.__put_config_to_file(config_data)
        elif destination == 'cache':
            config.set_config(config_data)
            return True


class Config(object):
    """
    配置文件，采用单例方式
    """
    __cache = {}
    __backup_cache = {}

    def __init__(self, config_file_path=BASE_DIR + SERVICE_BASE_CONFIG['meta_file']):
        self.__config_file_path = config_file_path
        self.__add_file_monitor()

    def get_config_file_path(self):
        return self.__config_file_path

    def set_config_file_path(self, file_path):
        self.__config_file_path = file_path

    def get_config(self):
        return self.__cache

    def set_config(self, config_data):
        self.__backup_cache = config_data
        self.__cache, self.__backup_cache = self.__backup_cache, self.__cache

    def refresh(self):
        """
        根据配置文件刷新缓存
        :return:
        """
        try:
            f = open(self.__config_file_path, 'r')
            self.__backup_cache = load(f, 'utf8')
            self.__cache, self.__backup_cache = self.__backup_cache, self.__cache
            app_log.info('__cache=' + str(self.__cache))
            app_log.info('__backup_cache=' + str(self.__backup_cache))
            f.close()
        except Exception as e:
            app_log.exception(e)

    def roll(self):
        """
        缓存中的数据回滚到上一个版本,回滚只支持一次
        :return:
        """
        self.__cache = self.__backup_cache or self.__cache

    def get_value(self, path, admin_id=None):
        if not path:
            return None
        default_key_list = filter(lambda key: key, '/'.join(('default', path)).split('/'))
        admin_key_list = filter(lambda key: key, '/'.join((str(admin_id), path)).split('/'))
        admin_value = self.__get_value_from_dict(self.__cache, admin_key_list)
        return admin_value if admin_value is not None else self.__get_value_from_dict(self.__cache,
                                                                                      default_key_list)

    def __get_value_from_dict(self, data_dict, key_list):
        """
        递归从字典中获取值
        :param data_dict:
        :param key_list:
        :return:
        """
        if not key_list or not data_dict:
            return None
        elif len(key_list) == 1:
            return data_dict[key_list[0]] if key_list[0] in data_dict else None
        else:
            return self.__get_value_from_dict(data_dict[key_list[0]], key_list[1:]) \
                if key_list[0] in data_dict else None


    def __add_file_monitor(self):
        """
        增加文件监听器，监听配置文件变更时间
        :return:
        """
        event_handler = ConfigFileEventHandler(self)
        observer = Observer()
        observer.schedule(event_handler, self.__config_file_path[:self.__config_file_path.rfind('/')], recursive=False)
        # observer.start()


config = Config()
config_holder = ConfigHolder()
# config模块在系统启动时会加载，注册中心注册也放在该模块中进行
register_center.register()

if __name__ == '__main__':
    config1 = Config()
    config2 = Config()
    print 'config1 == config2 ' + str(config1 == config2)
    # try:
    # while True:
    # time.sleep(1)
    # except KeyboardInterrupt:
    # ''

    print config.get_value('/version/', 'a1400000')
    print config.get_value('/version/')
    print config.get_value('/data_fetch_river/product/listen_get/fetcher/source', 'a1400000')
