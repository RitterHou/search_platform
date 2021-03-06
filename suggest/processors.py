# coding=utf-8

from suggest.data_processings import data_processing
from suggest.destinations import suggest_destination
from suggest.sources import suggest_source
from common.utils import get_dict_value_by_path
from common.loggers import app_log

__author__ = 'liuzhaoming'


class SuggestProcessor(object):
    """
    Suggest处理器，主要经过：source、data_processing、destination
    """

    def process(self, suggest_config, notification_data):
        app_log.info('Process is called, suggest_config={0} , notification_data={1}',
                     suggest_config['name'] if 'name' in suggest_config else '', notification_data)
        if suggest_source.is_iterable(suggest_config):
            self.__process_iteration(suggest_config, notification_data)
        else:
            self.__process_single(suggest_config, notification_data)

    def __process_iteration(self, suggest_config, notification_data):
        """
        处理需要迭代获取数据的情况
        :param suggest_config:
        :param notification_data:
        :return:
        """
        data_processing_config = get_dict_value_by_path('processing', suggest_config)
        has_next = True
        pos_from = 0
        notification_data['from'] = pos_from
        retry_times = 0
        suggest_term_dict = {}
        source_doc_dict = {}
        while has_next:
            try:
                source_docs = suggest_source.pull(suggest_config, notification_data, suggest_term_dict)
                if source_docs:
                    for source_doc in source_docs['root']:
                        source_doc_dict[source_doc['word']] = source_doc

                if pos_from == 0:
                    total = source_docs.get('total', 0)
                    if isinstance(total, dict):
                        total = total['value']
                cur_size = source_docs.get('curSize', 0)
                pos_from += cur_size
                notification_data['from'] = pos_from
                has_next = pos_from < total - 1
                if retry_times > 0:
                    retry_times = 0
            except Exception as e:
                app_log.error(
                    'suggest process iteration has error, suggest_config={0}, notification_data={1}', e, suggest_config,
                    notification_data)
                retry_times += 1
                if retry_times >= 2:
                    raise e

        for word, source_doc in source_doc_dict.iteritems():
            source_doc['hits'] = {'default': suggest_term_dict[word]}

        processed_data = data_processing.process_data(data_processing_config, {'root': source_doc_dict.values()},
                                                      suggest_config)
        suggest_destination.clear(suggest_config, processed_data)
        if processed_data:
            suggest_destination.push(suggest_config, processed_data)

    def __process_single(self, suggest_config, notification_data):
        data_processing_config = get_dict_value_by_path('processing', suggest_config)
        try:
            source_docs = suggest_source.pull(suggest_config, notification_data)
            processed_data = data_processing.process_data(data_processing_config, source_docs, suggest_config)
            suggest_destination.push(suggest_config, processed_data)
        except Exception as e:
            app_log.error(
                '__process_single has error, suggest_config={0}, notification_data={1}', e, suggest_config,
                notification_data)


suggest_processor = SuggestProcessor()
