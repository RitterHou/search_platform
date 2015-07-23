# coding=utf-8

from elasticsearch import ElasticsearchException

from common.adapter import es_adapter
from common.configs import config
from common.connections import EsConnectionFactory
from common.utils import merge, bind_dict_variable
from common.loggers import debug_log, app_log


__author__ = 'liuzhaoming'


class SuggestDestination(object):
    """
    将Suggest数据存储起来
    """

    @debug_log.debug('SuggestDestination.push')
    def push(self, destination_config, data):
        """
        将数据推到目的地，数据流的最后一步
        :param destination_config:
        :param data:
        :return:
        """
        app_log.info('Push is called')
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0}'.format(destination_config)
        __destination.push(destination_config, data)

    def clear(self, destination_config, data):
        destination_type = destination_config.get('destination_type', 'elasticsearch')
        __destination = DATA_DESTINATION_DICT.get(destination_type)
        assert __destination, 'the destination is not exist, destination_config={0}'.format(destination_config)
        __destination.clear(destination_config, data)


class ElasticsearchSuggestDestination(SuggestDestination):
    def push(self, destination_config, data):
        """
        将数据推到ES，数据流的最后一步
        :param destination_config:
        :param data:
        :param param:
        :return:
        """
        if 'reference' in destination_config:
            es_config = config.get_value('es_index_setting/' + destination_config['reference'])
            es_config = merge(es_config, destination_config)
            assert es_config, 'the reference is not exist, reference={0}'.format(destination_config)
        else:
            es_config = dict(destination_config)

        assert 'host' in es_config and 'index' in es_config and 'type' in es_config and 'id' in es_config, \
            'the es config is not valid, es_config={0}'.format(es_config)

        bind_dict_variable(es_config, data, False)

        operation = destination_config.get('operation', 'create')
        if not isinstance(data, list) and not isinstance(data, tuple):
            data = [data]
        if operation == 'create':
            es_adapter.batch_create(es_config, data)
        elif operation == 'update':
            es_adapter.batch_update(es_config, data)
        elif operation == 'delete':
            if not isinstance(data, (list, set, tuple)):
                data = [data]
            data = map(lambda item: item[0] if isinstance(item, (list, set, tuple)) and len(item) > 0 else item, data)
            data = filter(lambda item: 'id' in item, data)
            es_adapter.batch_delete(es_config, data)

    def clear(self, destination_config, param):
        """
        清除掉ES数据源中得所有数据
        :param destination_config:
        :param data:
        :return:
        """
        if 'reference' in destination_config:
            es_config = config.get_value('es_index_setting/' + destination_config['reference'])
            es_config = merge(es_config, destination_config)
            assert es_config, 'the reference is not exist, reference={0}'.format(destination_config)
        else:
            es_config = destination_config

        assert es_config['host'] and es_config['index'] and es_config['type'] and es_config[
            'id'], 'the es config is not valid, es_config={0}'.format(es_config)

        if 'clear_policy' not in destination_config or not destination_config.get('clear_policy'):
            return
        clear_policy = destination_config.get('clear_policy')
        if clear_policy == 'every_msg,all':
            if (isinstance(param, tuple) or isinstance(param, list)) and len(param) > 0:
                param = param[0]
            es_adapter.delete_all_doc(es_config, param)
        elif clear_policy == 'every_msg,auto_term':
            pass


class ElasticsearchProcessedSuggestDestination(ElasticsearchSuggestDestination):
    def push(self, destination_config, data_list):
        """
        将数据推到ES，数据流的最后一步
        :param destination_config:
        :param data:
        :param param:
        :return:
        """
        if not data_list:
            app_log.info("data_list is null")
            return None

        if 'reference' in destination_config:
            es_config = config.get_value('es_index_setting/' + destination_config['reference'])
            es_config = merge(es_config, destination_config)
            assert es_config, 'the reference is not exist, reference={0}'.format(destination_config)
        else:
            es_config = dict(destination_config)

        assert 'host' in es_config and 'index' in es_config and 'type' in es_config and 'id' in es_config, \
            'the es config is not valid, es_config={0}'.format(es_config)

        # bind_dict_variable(es_config, param, False)

        self.__batch_hybrid_es_opearate(es_config, data_list)

    def clear(self, destination_config, data_list):
        """
        清除掉ES数据源中得数据,目前支持两种策略，一种是清除掉所有；一种是清除掉自动分词形成的提示
        :param destination_config:
        :param data_list:
        :return:
        """
        if not data_list:
            app_log.info("data_list is null")
            return None

        if 'clear_policy' not in destination_config or not destination_config.get('clear_policy'):
            return

        if 'reference' in destination_config:
            es_config = config.get_value('es_index_setting/' + destination_config['reference'])
            es_config = merge(es_config, destination_config)
            assert es_config, 'the reference is not exist, reference={0}'.format(destination_config)
        else:
            es_config = dict(destination_config)

        assert 'host' in es_config and 'index' in es_config and 'type' in es_config and 'id' in es_config, \
            'the es config is not valid, es_config={0}'.format(es_config)

        data, param = data_list[0]
        clear_policy = destination_config.get('clear_policy')
        if clear_policy == 'every_msg,all':
            es_adapter.delete_all_doc(es_config, param)
        elif clear_policy == 'every_msg,auto_term':
            es_adapter.delete_by_query(es_config=es_config, doc=param, body={"query": {"term": {"source_type": "1"}}})


    def __batch_hybrid_es_opearate(self, es_config, data_list):
        bulk_body_list = []
        for (data, param) in data_list:
            index, es_type, doc_id = es_adapter.get_es_doc_keys(es_config, kwargs=param)
            operation = es_config.get('operation', 'create')
            if operation == 'delete':
                bulk_body_list.append({"delete": {"_index": index, "_type": es_type, "_id": doc_id}})
            else:
                if 'script' in data:  # 只有update操作才支持脚本
                    bulk_body_list.append({"update": {"_index": index, "_type": es_type, "_id": doc_id}})
                    bulk_body_list.append(data)
                else:
                    bulk_body_list.append({"index": {"_index": index, "_type": es_type, "_id": doc_id}})
                    bulk_body_list.append(data)

        es_connection = EsConnectionFactory.get_es_connection(
            es_config=dict(es_config, index=index, type=es_type, version=config.get_value('version')))
        try:
            es_bulk_result = es_connection.bulk(bulk_body_list)
            es_adapter.process_es_bulk_result(es_bulk_result)
        except ElasticsearchException as e:
            app_log.error('es operation input param is {0}', e, list(bulk_body_list))
            app_log.exception(e)


DATA_DESTINATION_DICT = {'elasticsearch': ElasticsearchSuggestDestination(),
                         'elasticsearch_processed': ElasticsearchProcessedSuggestDestination()}


class DestinationHelp(object):
    destination = SuggestDestination()

    @debug_log.debug('SuggestDestinationHelp.push')
    def push(self, river_config, data):
        """
        将数据推到目的地
        :param river_config:
        :param data:
        :return:
        """
        if not data:
            app_log.info('data is null')
            return
        if 'destination' not in river_config:
            app_log.warning("The test_river doesn't have destination, river_config={0}".format(river_config))
            return

        destination_config_list = river_config['destination']
        for destination_config in destination_config_list:
            try:
                self.destination.push(destination_config, data)
            except Exception as e:
                app_log.error("process message has error, destination_config={0}, data={1}", e,
                              destination_config, data)

    def clear(self, river_config, data):
        """
        清除掉所有数据
        :param river_config:
        :param data:
        :return:
        """
        if 'destination' not in river_config:
            app_log.warning("The test_river doesn't have destination, river_config={0}".format(river_config))

        destination_config_list = river_config['destination']
        for destination_config in destination_config_list:
            try:
                self.destination.clear(destination_config, data)
            except Exception as e:
                app_log.error("process message has error, destination_config={0}, data={1}", e,
                              destination_config, data)


suggest_destination = DestinationHelp()