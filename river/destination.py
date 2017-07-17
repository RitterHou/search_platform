# -*- coding: utf-8 -*-
from common.adapter import es_adapter
from common.es_routers import es_router
from common.loggers import app_log
from river import do_msg_process_error

__author__ = 'liuzhaoming'


class DataDestination(object):
    """
    数据流目的地
    """

    def push(self, destination_config, data, param=None):
        """
        将数据推到目的地，数据流的最后一步
        :param destination_config:
        :param data:
        :param param
        :return:
        """
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0} , param={1}'.format(
            destination_config, param)
        __destination.push(destination_config, data, param)

    def clear(self, destination_config, data, param=None):
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0} , param={1}'.format(
            destination_config, param)
        __destination.clear(destination_config, data, param)


class ElasticSearchDestination(DataDestination):
    """
    数据流目的地为Elasticsearch
    """

    def push(self, destination_config, data, param=None):
        """
        将数据推到ES中，数据流的最后一步
        :param destination_config:
        :param data:
        :param param
        :return:
        """
        es_config = es_router.merge_es_config(destination_config)


        if not isinstance(data, (list, tuple)):
            data = [data]
        _param = param.get('fields') if param else {}
        input_param = dict(data[0], **_param)
        es_config = es_router.route(es_config, input_param=input_param)
        operation = es_config.get('operation', 'create')
        self._add_private_field(es_config, data, input_param)
        if operation == 'create':
            es_adapter.batch_create(es_config, data, input_param)
        elif operation == 'update':
            es_adapter.batch_update(es_config, data, input_param)
        elif operation == 'delete':
            es_adapter.batch_delete(es_config, data, input_param)
        elif operation == 'ids_same_prop_update':
            es_adapter.batch_update_with_props_by_ids(es_config, data, input_param)

    def clear(self, destination_config, data, param=None):
        """
        清除掉ES数据源中得所有数据
        :param destination_config:
        :param data:
        :return:
        """
        if data is None:
            app_log.warning('destination clear fail, because data is null, {0}', destination_config)
            return
        if data == {} or data == [] or data == ():
            app_log.warning('destination clear maybe fail, because data is empty, {0}', destination_config)
            data = [{}]
        es_config = es_router.merge_es_config(destination_config)

        if not isinstance(data, (list, tuple)):
            data = [data]

        es_config = es_router.route(es_config, input_param=data[0])

        if 'clear_policy' not in es_config or not es_config.get('clear_policy'):
            return
        clear_policy = es_config.get('clear_policy')
        data = data[0] if len(data) > 0 else data
        data = data or {}
        if param:
            param = param['fields'] if 'fields' in param else param
            data = dict(data, **param)
        if clear_policy == 'every_msg,all':
            es_adapter.delete_all_doc_by_type(es_config, data)
        elif clear_policy == 'every_msg,by_adminId':
            if not data.get('adminId'):
                app_log.error('destination clear fail, because adminId is null {0}', data)
                return

            es_adapter.delete_by_field(es_config, data, '_adminId', data['adminId'])

    def _add_private_field(self, es_config, data_list, param):
        """
        添加搜索平台私有字段，主要是将adminId作为私有字段添加到数据结构中
        :param es_config
        :param data_list:
        :param param:
        :return:
        """
        if not param or not data_list or not es_config.get('add_admin_id_field'):
            return

        if not param.get('adminId'):
            return
        admin_id = param['adminId']
        for item in data_list:
            item['_adminId'] = admin_id
DATA_DESTINATION_DICT = {'elasticsearch': ElasticSearchDestination()}


class DestinationHelp(object):
    destination = DataDestination()

    def push(self, river_config, data, param=None):
        if not data:
            app_log.info('data is null')
            return
        if 'destination' not in river_config:
            app_log.warning("The test_river doesn't have destination, river_config={0}", river_config)
            return

        destination_config_list = river_config['destination']
        for destination_config in destination_config_list:
            try:
                self.destination.push(destination_config, data, param)
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