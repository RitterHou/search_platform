# -*- coding: utf-8 -*-
from common.es_routers import es_router
from river import do_msg_process_error

from common.adapter import es_adapter
from common.loggers import debug_log, app_log
__author__ = 'liuzhaoming'


class DataDestination(object):
    """
    数据流目的地
    """

    def push(self, destination_config, data):
        """
        将数据推到目的地，数据流的最后一步
        :param destination_config:
        :param data:
        :return:
        """
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0}'.format(destination_config)
        __destination.push(destination_config, data)

    def clear(self, destination_config, data, param=None):
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0}'.format(destination_config)
        __destination.clear(destination_config, data)


class ElasticSearchDestination(DataDestination):
    """
    数据流目的地为Elasticsearch
    """

    def push(self, destination_config, data):
        """
        将数据推到ES中，数据流的最后一步
        :param destination_config:
        :param data:
        :return:
        """
        es_config = es_router.merge_es_config(destination_config)


        operation = destination_config.get('operation', 'create')
        if not isinstance(data, (list, tuple)):
            data = [data]
        if operation == 'create':
            es_adapter.batch_create(es_config, data)
        elif operation == 'update':
            es_adapter.batch_update(es_config, data)
        elif operation == 'delete':
            es_adapter.batch_delete(es_config, data)
        elif operation == 'ids_same_prop_update':
            es_adapter.batch_update_with_props_by_ids(es_config, data)

    def clear(self, destination_config, data, param=None):
        """
        清除掉ES数据源中得所有数据
        :param destination_config:
        :param data:
        :return:
        """
        es_config = es_router.merge_es_config(destination_config)

        assert es_config['host'] and es_config['index'] and es_config['type'] and es_config[
            'id'], 'the es config is not valid, es_config={0}'.format(es_config)

        if 'clear_policy' not in destination_config or not destination_config.get('clear_policy'):
            return
        clear_policy = destination_config.get('clear_policy')
        if clear_policy == 'every_msg,all':
            if (isinstance(data, tuple) or isinstance(data, list)) and len(data) > 0:
                data = data[0]
            elif not data:
                data = {}
            if param:
                data = dict(data, param)
            es_adapter.delete_all_doc_by_type(es_config, data)



DATA_DESTINATION_DICT = {'elasticsearch': ElasticSearchDestination()}


class DestinationHelp(object):
    destination = DataDestination()

    @debug_log.debug('DestinationHelp.push')
    def push(self, river_config, data):
        if not data:
            app_log.info('data is null')
            return
        if 'destination' not in river_config:
            app_log.warning("The test_river doesn't have destination, river_config={0}", river_config)
            return

        destination_config_list = river_config['destination']
        for destination_config in destination_config_list:
            try:
                self.destination.push(destination_config, data)
            except Exception as e:
                app_log.error("Process message has error, destination_config={0}, data={1}", e,
                              destination_config, data)
                do_msg_process_error(e)

    def clear(self, river_config, data, param=None):
        if 'destination' not in river_config:
            app_log.warning("The test_river doesn't have destination, river_config={0}", river_config)

        destination_config_list = river_config['destination']
        for destination_config in destination_config_list:
            try:
                self.destination.clear(destination_config, data, param)
            except Exception as e:
                app_log.error("Process message has error, destination_config={0}, data={1}, param={2}", e,
                              destination_config, data, param)
                do_msg_process_error(e)


destination = DestinationHelp()