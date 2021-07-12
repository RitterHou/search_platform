# -*- coding: utf-8 -*-

from itertools import chain
import time

from common.adapter import es_adapter
from common.admin_config import admin_config
from common.configs import config
from common.connections import EsConnectionFactory, Es7ConnectionFactory
from common.es_routers import es_router
from filters import notification_filter
from common.utils import get_dict_value_by_path, hash_encode
from common.loggers import app_log
from search_platform.celery_config import app
from suggest.processors import suggest_processor

__author__ = 'liuzhaoming'


@app.task(bind=True)
def process_suggest_notification(self, suggest_config, notification_data_list):
    """
    通过celery框架执行suggest任务
    :param suggest_config:
    :param notification_data_list:
    :return:
    """
    for notification_data in notification_data_list:
        try:
            suggest_processor.process(suggest_config, notification_data)
        except Exception as ex:
            app_log.error(
                'process celery suggest message has error, message={0}, river_key={1}', ex, suggest_config,
                notification_data)


def process_no_celery_suggest_notification(suggest_config, notification_data_list):
    """
    立即执行suggest任务
    :param suggest_config:
    :param notification_data_list:
    :return:
    """
    for notification_data in notification_data_list:
        try:
            suggest_processor.process(suggest_config, notification_data)
        except Exception as ex:
            app_log.error(
                'process suggest message has error, message={0}, river_key={1}', ex, suggest_config, notification_data)


class SuggestNotification(object):
    """
    Suggest消息触发器，目前支持定时任务触发
    """

    # @distributed_lock.lock('SuggestNotification.notification')
    def notify(self, notification_config, suggest_config, notify_data=None):
        try:
            app_log.info('Notify is called, notification_config={0}', notification_config)
            notification_type = get_dict_value_by_path('type', notification_config, 'elasticsearch_regularly_scan')
            _suggest_notification = NOTIFICATION_DICT.get(notification_type)
            if not _suggest_notification:
                app_log.error('Unsupport suggest notification, the config is {0}', notification_config)
                return
            notification_data_list = _suggest_notification.notify(notification_config, suggest_config, notify_data)
            app_log.info('Notify data list is {0}', notification_data_list)
            if notification_type == 'elasticsearch_regularly_scan':
                # process_suggest_notification('', suggest_config, notification_data_list)
                process_suggest_notification.delay(suggest_config, notification_data_list)
                # 因为任务是通过celery异步执行，所以延迟10秒，防止结束过快释放锁过快，其它进程上的相同任务得以进行
                time.sleep(10)
            else:
                process_no_celery_suggest_notification(suggest_config, notification_data_list)
        except Exception as ex:
            app_log.error('Suggest Notification notify has error ', ex)


class AdminSuggestNotification(SuggestNotification):
    """
    固定Admin ID suggest初始化
    """

    def notify(self, notification_config, suggest_config, notify_data):
        _es_config = notification_config.get('es_cfg')
        if not _es_config:
            app_log.error('notification config is invalid, because the es_cfg is None : {0}', notification_config)
            return []
        _es_config = es_router.merge_es_config(_es_config)
        notification_config['es_cfg'] = _es_config
        modulus = suggest_config['source']['hashcode']['modulus']
        notify_data['hashcode'] = hash_encode(notify_data.get('adminId'), modulus=modulus)
        is_vip = admin_config.is_vip(notify_data.get('adminId'))
        index, doc_type, doc_id = es_adapter.get_es_doc_keys(_es_config, kwargs=notify_data)
        if _es_config.get('version') == 7:
            _es_connection = Es7ConnectionFactory.get_es_connection(host=_es_config['host'])
            return [{'index': index, 'hashcode': not is_vip, 'adminId': notify_data['adminId']}]
        else:
            _es_connection = EsConnectionFactory.get_es_connection(host=_es_config['host'])
            if not _es_connection.indices.exists_type(index, doc_type):
                return []
            return [{'index': index, 'type': doc_type, 'hashcode': not is_vip, 'adminId': notify_data['adminId']}]


class EsRegularlyScanNotification(SuggestNotification):
    """
    ES全库所有索引扫描触发器
    """

    def notify(self, notification_config, suggest_config, notify_data=None):
        _host = notification_config.get('host')
        if not _host:
            app_log.error('notification config is invalid : {0}', notification_config)
            return

        variable_values = config.get_value('consts/custom_variables')
        _host = _host.format(**variable_values)
        _es_connection = EsConnectionFactory.get_es_connection(host=_host)
        mapping_dict = _es_connection.indices.get_mapping()
        index_type_dict_list = list(chain(
            *[self.__parse_index_mapping(index_name, mapping_dict[index_name]) for index_name in mapping_dict]))
        filter_config = notification_config.get('filter', {})
        matched_index_type_dict_list = filter(
            lambda index_type_dict: notification_filter.filter(index_type_dict, filter_config), index_type_dict_list)
        return matched_index_type_dict_list

    def __parse_index_mapping(self, index_name, mapping):
        """
        解析索引mapping文件，获取(index, type)列表
        :param index_name:
        :param mapping:
        :return:
        """
        if not mapping or not index_name:
            return ()

        __mappings = mapping.get('mappings')
        if not __mappings:
            return ()
        return [{'index': index_name, 'type': type_name} for type_name in mapping.get('mappings')]


class EsVipHashScanNotification(SuggestNotification):
    pass


NOTIFICATION_DICT = {'elasticsearch_regularly_scan': EsRegularlyScanNotification(),
                     'elasticsearch_admin': AdminSuggestNotification()}

if __name__ == '__main__':
    suggest_config = {u'notification': {u'filter': {u'conditions': [
        {u'operator': u'is', u'field': u'index', u'type': u'regex',
         u'expression': u'^search_platform-gonghuo[\\d\\D]*'},
        {u'operator': u'is', u'field': u'type', u'type': u'regex', u'expression': u'^Product$'}], u'type': u'es_regex',
        u'union_operator': u'and'}, u'host': u'http://172.19.65.66:9200',
        u'type': u'elasticsearch_regularly_scan',
        u'crontab': {u'second': 0, u'hour': 13, u'minute': 10},
        u'key': u'elasticsearch_regularly_scan_1'}, u'destination': [
        {u'index': u'suggest-gonghuo-{version}', u'operation': u'create', u'reference': u'suggest',
         u'destination_type': u'elasticsearch_processed'}], u'processing': {
        u'output': {u'common_fields': {u'fields': {u'id': u'id'}, u'type': u'map'},
                    u'payloads': {u'fields': {u'source_type': u'source_type', u'hits': u'hits'}, u'type': u'map'},
                    u'weight': {u'type': u'script',
                                u'param': {u'fields': {u'current_weight': u'int((hits*0.25 + source_type_weight)/2)'},
                                           u'type': u'math_expression'}, u'language': u'mvel',
                                u'script': u'ctx._source.suggest.weight = round(ctx._source.suggest.weight*0.5 + 0.5*current_weight)'}},
        u'type': u'basic_processing'},
        u'source': {u'param_parser': {u'fields': {}, u'type': u'regex'}, u'type': u'iterator_es_get',
                    u'data_parser': {
                        u'fields': {u'category': u'type', u'brand': u'brand', u'title': u'title'},
                        u'keyword_filter_regex': u'^[\u4e00-\u9fa5A-Za-z][\u4e00-\u9fa5A-Za-z0-9]+$',
                        u'type': u'map'}, u'size': 50}}
    notfication_data_list = [{'index': u'qmshop-gonghuo-1.0.0', 'type': u'Product'}]

    process_suggest_notification('kk', suggest_config, notfication_data_list)

    data = {'source_type': '1', 'hits': 73, 'word': u'\u6c34\u679c', 'id': '\\u6c34\\u679c', 'source_type_weight': 1}
    data = {'hits': 73, 'source_type_weight': 1}

    weight_config = {u'type': u'script', u'language': u'mvel',
                     u'param': {u'fields': {u'current_weight': u'int((hits*0.25 + source_type_weight)/2)'},
                                u'type': u'math_expression'},
                     u'script': u'ctx._source.suggest.weight = round(ctx._source.suggest.weight*0.5 + 0.5*current_weight)'}
    try:
        if 'param' in weight_config and weight_config['param']:
            if weight_config['param'].get('type') == 'math_expression':
                kk = dict(data)
                eval(u'int((hits*0.25 + source_type_weight)/2)', kk)
                # script_param_list = [(key, eval(value, data)) for (key, value) in
                # weight_config['param']['fields'].iteritems()]
                # print script_param_list
                print "1"
    except Exception as e:
        print e

    g = {'b': 2}
    result = eval('b+2', None, g)
    print result

    es_config = {u'index': u'suggest-gonghuo-1.0.0', u'reference': u'suggest', u'mapping': {u'properties': {
        u'suggest': {u'search_analyzer': u'simple', u'index_analyzer': u'simple', u'preserve_separators': True,
                     u'payloads': True, u'max_input_length': 50, u'preserve_position_increments': True,
                     u'type': u'completion'}, u'id': {u'index': u'not_analyzed', u'type': u'string', u'store': True},
        u'name': {u'index': u'not_analyzed', u'type': u'string', u'store': True}}},
                 u'host': u'http://172.19.65.66:9200',
                 'version': u'1.0.0', u'destination_type': u'elasticsearch_processed', u'operation': u'create',
                 u'type': u'ProductSuggest', u'id': u'{id}'}
    es_connection = EsConnectionFactory.get_es_connection(es_config=es_config)
    bulk_body_list = [{"update": {"_type": "ProductSuggest", "_id": "mark", "_index": "suggest-gonghuo-1.0.0"}},
                      {"detect_noop": True, "params": {"current_weight": 9},
                       "script": "ctx._source.suggest.weight = round(ctx._source.suggest.weight*0.5 + 0.5*current_weight)"}
                      ]

    # es_bulk_result = es_connection.bulk(bulk_body_list)
    # print es_bulk_result
