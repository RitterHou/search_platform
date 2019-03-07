# -*- coding: utf-8 -*-
# Create your models here.
import time
from collections import OrderedDict
from itertools import chain

from algorithm.content_based_recom import content_recom
from algorithm.like_query_string import like_str_algorithm
from algorithm.section_partitions import equal_section_partitions
from common.adapter import es_adapter
from common.configs import config
from common.connections import EsConnectionFactory
from common.exceptions import InvalidParamError, EsConnectionError
from common.loggers import debug_log, query_log as app_log
from common.pingyin_utils import pingyin_utils
from common.utils import unbind_variable, deep_merge, bind_variable, get_default_es_host, get_dict_value, get_cats_path, \
    get_day_and_hour
from dsl_parser import qdsl_parser, extend_parser, DEFAULT_VALUE
from measure.measure_units import measure_unit_helper
from search_platform import settings
from service.search_scenes import spu_search_scene

__author__ = 'liuzhaoming'
BATCH_REQUEST_TIMEOUT = config.get_value('consts/global/es_conn_param/batch_request_timeout') or 30
BATCH_TIMEOUT = config.get_value('consts/global/es_conn_param/batch_timeout') or 120000
INDEX_REQUEST_TIMEOUT = config.get_value('consts/global/es_conn_param/index_request_timeout') or 120
INDEX_TIMEOUT = config.get_value('consts/global/es_conn_param/index_timeout') or 120000


def get_es_search_params(es_config, index_name, doc_type, args, parse_fields=None):
    """
    获取ES search参数
    :param es_config:
    :param index_name:
    :param doc_type:
    :param args:
    :param parse_fields:
    :return:
    """
    params = {}
    if not args:
        return params
    "处理preference, restful入参为：search_preference=kk"
    preference = args.get("search_preference")
    if preference in ['_primary', '_primary_first', '_local', '_only_nodes']:
        params['preference'] = preference
    elif preference == '_auto' and parse_fields and parse_fields.get('adminId'):
        cur_time_str = get_day_and_hour()
        params['preference'] = '-'.join((parse_fields.get('adminId'), cur_time_str))
    return params


class EsModel(object):
    field_config = {}

    def __init__(self, **args):
        self.__dict__.update([(key, value) for (key, value) in args.iteritems() if key in self.field_config])


class EsProductManager(object):
    connection_pool = EsConnectionFactory

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        通过ES查询产品数据
        :param index_name:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        if not es_connection:
            raise EsConnectionError()
        start_time = time.time()
        qdsl = qdsl_parser.get_product_query_qdsl(es_config, index_name, doc_type, args, parse_fields, es_connection)
        qdsl_end_time = time.time()
        app_log.info('Get product dsl finish, spend time {4}, index={0} , type={1} , args={2}, dsl={3}', index_name,
                     doc_type, args, qdsl, qdsl_end_time - start_time)
        if args.get('scene') == 'spu_aggs':
            # 根据sku聚合搜索spu场景
            es_search_params = get_es_search_params(es_config, index_name, doc_type, args, parse_fields)
            result, es_agg_result = spu_search_scene.get_spu_by_sku(qdsl, es_config, args, parse_fields,
                                                                    es_search_params)
        else:
            if args.get('ex_body_type') == 'scroll':
                es_result = self.__scroll_search(qdsl, es_config, index_name, doc_type, args, parse_fields)
            elif args.get('ex_body_type') == 'scan':
                es_result = self.__scan_search(qdsl, es_config, index_name, doc_type, args, parse_fields)
            else:
                es_search_params = get_es_search_params(es_config, index_name, doc_type, args, parse_fields)
                es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl,
                                                 **es_search_params)
            es_end_time = time.time()
            app_log.info('Elasticsearch search index={0} , type={1} , spend time {2}', index_name, doc_type,
                         es_end_time - qdsl_end_time)
            result = self.parse_es_result(es_result, args)
            app_log.info('Parse elasticsearch search result index={0} , type={1} , spend time {2}', index_name,
                         doc_type, time.time() - es_end_time)

        return result

    def save(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        保存商品
        :param es_config:
        :param product:
        :return:
        """
        app_log.info(
            'Product save is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if not product:
            app_log.error('Product save input product is invalid')
            raise InvalidParamError()

        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        # doc_id = bind_variable(es_config['id'], product)
        _bulk_body = self._build_index_body(es_config, index_name, doc_type, product, parse_fields, timestamp)
        if not _bulk_body:
            return
        if redo:
            _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                     timestamp)
        es_bulk_result = es_connection.bulk(_bulk_body,
                                            params={'request_timeout': BATCH_REQUEST_TIMEOUT, 'timeout': BATCH_TIMEOUT})
        return es_adapter.get_es_bulk_result(es_bulk_result)

    def _filter_has_update_doc(self, es_config, index_name, doc_type, es_connection, _bulk_body, timestamp,
                               es_op_name='index'):
        """
        过滤掉已经被修改的文档记录
        :param es_config:
        :param index_name:
        :param doc_type:
        :param es_connection:
        :param _bulk_body:
        :param timestamp
        :param es_op_name es bulk操作类型， index, update, delete
        :return:
        """
        doc_id_template = 'generate' if 'id' not in es_config else es_config['id']
        if doc_id_template == 'generate':
            return _bulk_body
        doc_id_list = []
        es_exist_doc_list = []
        for bulk_body_item in _bulk_body:
            if es_op_name in bulk_body_item and isinstance(bulk_body_item[es_op_name], dict) \
                    and '_id' in bulk_body_item[es_op_name]:
                doc_id_list.append(bulk_body_item[es_op_name]['_id'])
        if doc_id_list:
            ids_query_dsl = {'query': {'ids': {'values': doc_id_list}}}
            search_result = es_connection.search(index=index_name, doc_type=doc_type, body=ids_query_dsl)
            es_exist_doc_list = map(lambda search_result_item:
                                    {'id': search_result_item['_id'],
                                     '_update_time': search_result_item['_source']['_update_time']
                                     if '_update_time' in search_result_item['_source'] else 0},
                                    search_result['hits']['hits'])
        no_need_operate_doc_ids = map(lambda es_doc: es_doc['id'],
                                      filter(lambda es_doc: es_doc['_update_time'] >= timestamp, es_exist_doc_list))
        if no_need_operate_doc_ids:
            _temp_bulk_item_list = []
            step = 1 if es_op_name == 'delete' else 2
            for index in xrange(0, len(_bulk_body), step):
                bulk_body_item = _bulk_body[index]
                if es_op_name in bulk_body_item and bulk_body_item[es_op_name]['_id'] not in no_need_operate_doc_ids:
                    _temp_bulk_item_list.append(bulk_body_item)
                    if step == 2:
                        _temp_bulk_item_list.append(_bulk_body[index] + 1)
            _bulk_body = _temp_bulk_item_list
        return _bulk_body

    def _build_index_body(self, es_config, index_name, doc_type, product, parse_fields, timestamp):
        """
        创建批量index数据结构
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :return:
        """
        _bulk_body = []
        doc_id_template = 'generate' if 'id' not in es_config else es_config['id']
        if not isinstance(product, (list, tuple, set)):
            batch_product_list = [product]
        else:
            batch_product_list = product
        self._add_private_field(es_config, batch_product_list, parse_fields)
        for item_product in batch_product_list:
            es_index_info = {"index": {"_index": index_name, "_type": doc_type}} if doc_id_template == 'generate' \
                else {
                "index": {"_index": index_name, "_type": doc_type,
                          "_id": bind_variable(es_config['id'], item_product)}}
            item_product['_update_time'] = timestamp
            _bulk_body.append(es_index_info)
            _bulk_body.append(item_product)
        return _bulk_body

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
        if 'adminId' not in param or not param['adminId']:
            return
        admin_id = param['adminId']
        for item in data_list:
            item['_adminId'] = admin_id

    def update(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        更新商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :return:
        """
        app_log.info(
            'Product update is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if not product:
            app_log.error('Product update input product is invalid')
            raise InvalidParamError()

        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        if parse_fields and 'id' in parse_fields and parse_fields['id']:
            doc_id = parse_fields['id']
            _bulk_body = self._build_update_body(es_config, index_name, doc_type, product, parse_fields, timestamp,
                                                 doc_id)
        else:
            _bulk_body = self._build_update_body(es_config, index_name, doc_type, product, parse_fields, timestamp)
        if redo:
            _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                     timestamp, 'update')
        es_bulk_result = es_connection.bulk(_bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                'timeout': BATCH_TIMEOUT})
        return es_adapter.get_es_bulk_result(es_bulk_result)

    def _build_update_body(self, es_config, index_name, doc_type, product, parse_fields, timestamp, doc_id=None):
        """
        构造ES 批量update结构
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :param doc_id
        :return:
        """
        _bulk_body = []

        if not isinstance(product, (list, tuple, set)):
            batch_product_list = [product]
        else:
            batch_product_list = product
        self._add_private_field(es_config, batch_product_list, parse_fields)
        for item_product in batch_product_list:
            item_product['_update_time'] = timestamp
            es_update_info = {
                "update": {"_index": index_name, "_type": doc_type,
                           "_id": bind_variable(es_config['id'], item_product) if not doc_id else doc_id}}
            _bulk_body.append(es_update_info)
            _bulk_body.append({'doc': item_product})
        return _bulk_body

    def delete(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        删除商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :param redo
        :return:
        """

        def parse_delete_doc_ids():
            if parse_fields and 'id' in parse_fields and parse_fields['id']:
                doc_id = parse_fields['id']
            elif 'doc_id' in product:
                doc_id = product['doc_id']
            else:
                if isinstance(product, (list, tuple, set)):
                    doc_id = map(lambda item: bind_variable(es_config['id'], item), product)
                else:
                    doc_id = bind_variable(es_config['id'], product)
            if not isinstance(doc_id, (list, tuple, set)):
                _doc_id_list = doc_id.split(',')
            else:
                _doc_id_list = doc_id
            return _doc_id_list

        app_log.info(
            'Product delete is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if product is None:
            app_log.error('Product delete input product is invalid')
            raise InvalidParamError()
        if product.get('ex_body_type') == 'scroll':
            # 表示是删除scroll缓存
            return self.__delete_scroll_cache(es_config, index_name, doc_type, product, parse_fields)
        else:
            es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
            if not es_connection:
                raise EsConnectionError()

            doc_id_list = parse_delete_doc_ids()
            _bulk_body = map(lambda _doc_id: {'delete': {'_index': index_name, '_type': doc_type, '_id': _doc_id}},
                             doc_id_list)

            if redo:
                _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                         timestamp, 'delete')
            es_bulk_result = es_connection.bulk(_bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                    'timeout': BATCH_TIMEOUT})
            return es_adapter.get_es_bulk_result(es_bulk_result)

    def parse_es_result(self, es_result, args):
        """
        解析ES查询结果
        :param es_result:
        :return:
        """
        multi_field_dict = {}
        if 'hits' in es_result and es_result['hits'] and 'hits' in es_result['hits']:
            total = es_result['hits']['total']
            doc_list = es_result['hits']['hits']
            product_list = map(lambda doc: self.parse_es_result_item(doc, args, multi_field_dict), doc_list)
        elif '_source' in es_result:
            total = 1
            product_list = [self.parse_es_result_item(es_result, args, multi_field_dict)]
        else:
            total = 0
            product_list = []
        result = {'root': product_list, 'total': total}
        if '_scroll_id' in es_result and es_result['_scroll_id']:
            # 如果是scroll查询，那么需要返回_scroll_id
            result['_scroll_id'] = es_result['_scroll_id']

        return result

    def parse_es_result_item(self, es_result_item, args, multi_field_dict):
        """
        解析单个ES search 结果
        :param es_result_item:
        :return:
        """
        doc = None
        if '_source' in es_result_item:
            doc = es_result_item['_source']
        elif 'fields' in es_result_item:
            fields_result = es_result_item['fields']
            doc = {}
            for field_name in fields_result:
                if fields_result[field_name]:
                    doc[field_name] = fields_result[field_name][0]

        self.__parse_hight_result(es_result_item, args, multi_field_dict, doc)
        return doc

    def __parse_hight_result(self, es_result_item, args, multi_field_dict, doc):
        """
        解析高亮字段
        :param es_result_item:
        :param args:
        :param multi_field_dict:
        :param doc:
        :return:
        """
        if not doc or 'highlight' not in es_result_item:
            return
        for (key, value) in es_result_item['highlight'].iteritems():
            if key == '_all':
                continue
            if key not in multi_field_dict:
                origin_field_name = extend_parser.get_highlight_field_to_origin(args, key)
                if origin_field_name not in doc:
                    continue
                multi_field_dict[key] = origin_field_name
            if isinstance(value, (tuple, list, set)) and not isinstance(doc[multi_field_dict[key]],
                                                                        (tuple, list, set)):
                doc[multi_field_dict[key]] = value[0]
            else:
                doc[multi_field_dict[key]] = value

    def __scroll_search(self, qdsl, es_config, index_name, doc_type, args, parse_fields=None):
        """
        scroll查询
        第一次执行参数：ex_body_type=scroll&scroll_time=1m&search_type=scan
        ex_body_type参数 scroll值表示使能scroll搜索，必填
        scroll_time表示缓存保存时间，可以不填，默认为系统参数/consts/query/scroll_time
        search_type 为scan，如果配置了该参数，搜索时不会进行排序，会大大加快执行速度
        返回结果会带有"_scroll_id"字段，下一次请求是需要带上该字段
        后续执行参数 ex_body_type=scroll &_scroll_id=wierowirowirowiroiwr & scroll_time=1m
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        scroll_time = args.get('scroll_time') or config.get_value('/consts/query/scroll_time')
        return es_adapter.scroll(scroll_time, qdsl, search_type=args.get('search_type'),
                                 scroll_id=args.get('_scroll_id'), index=index_name, doc_type=doc_type,
                                 host=es_config.get('host'))

    def __scan_search(self, qdsl, es_config, index_name, doc_type, args, parse_fields=None):
        """
        scan查询，查询所有符合条件的数据，不进行排序
        :param qdsl:
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        scroll_time = args.get('scroll_time') or config.get_value('/consts/query/scroll_time')
        iter_es_scan_result = es_adapter.scan(scroll_time, qdsl, index=index_name, doc_type=doc_type,
                                              host=es_config.get('host'))
        es_result = {'hits': {'hits': []}}
        iter_es_scan_result = list(iter_es_scan_result)
        for item_scan_result in iter_es_scan_result:
            if item_scan_result:
                es_result['hits']['hits'].append(item_scan_result)
        es_result['hits']['total'] = len(iter_es_scan_result)
        return es_result

    def __delete_scroll_cache(self, es_config, index_name, doc_type, product, parse_fields):
        """
        删除scroll缓存
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields:
        :return:
        """
        return es_adapter.delete_scroll(product.get('_scroll_id'), index=index_name, doc_type=doc_type,
                                        host=es_config.get('host'))


class EsAggManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsAggManager.get::')
    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        查询聚合数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_agg_qdl(es_config, index_name, doc_type, args, parse_fields, es_connection)
        app_log.info('Get agg dsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args, qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result, args)
        range_result = self.get_agg_range_result(es_config, index_name, doc_type, args, es_result, qdsl)
        if range_result:
            result = deep_merge(result, range_result)
        app_log.info('EsAggManager get return is {0}', result)
        return result

    def parse_es_result(self, es_result, args=None):
        """
        解析ES返回结果
        :param es_result:
        :param args
        :return:
        """
        result = {}
        if 'aggregations' not in es_result:
            return result

        agg_result = es_result['aggregations']
        is_last_cat = self.__is_last_cat(agg_result)
        is_ignore_cat = self.__is_ignore_cat(args)
        for agg_key in agg_result:
            if agg_key == 'brand':
                result['brand'] = self.__parse_nomal_agg_result(agg_result, 'brand')
            elif agg_key == 'cats':
                result['cats'] = self.__parse_cats_agg_result(agg_result, 'cats', is_last_cat)
            elif agg_key == 'props':
                result['props'] = self.__parse_prop_agg_result(agg_result, 'props', is_last_cat, is_ignore_cat)
            elif agg_key.startswith('ex_agg_'):
                if agg_key.endswith('.cats'):
                    result[agg_key] = self.__parse_cats_agg_result(agg_result, agg_key, is_last_cat)
                elif agg_key.endswith('.key_value'):
                    result[agg_key] = self.__parse_key_value_agg_result(agg_result, agg_key)
                else:
                    result[agg_key] = agg_result[agg_key]

        return result

    def get_agg_range_result(self, es_config, index_name, doc_type, args, es_result, qdsl):
        """
        进行聚合 range查询
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        start_time = time.time()
        if 'aggregations' not in es_result or not es_result['aggregations']:
            return
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        agg_range_qdsl = extend_parser.get_section_agg_range_dsl(args, es_result['aggregations'])
        if not agg_range_qdsl:
            return
        if qdsl and 'aggs' in qdsl:
            del qdsl['aggs']

        cur_qdsl = deep_merge(qdsl if qdsl else {}, agg_range_qdsl)
        app_log.info('Get agg range qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     cur_qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=cur_qdsl)
        result = self.parse_es_agg_range_result(es_result, args)
        app_log.info('EsAggManager get agg range return is {0}', result)
        debug_log.print_log('get_agg_range_result spends {0}'.format(time.time() - start_time))
        return result

    def parse_es_agg_range_result(self, es_result, query_params):
        """
        解析ES range聚合查询结果
        :param es_result:
        :param query_params:
        :return:
        """
        start_time = time.time()
        if 'aggregations' not in es_result or not es_result['aggregations']:
            return
        ex_query_params_dict = extend_parser.get_query_params_by_prefix(query_params, 'ex_section_')
        total_doc_count = es_result['hits']['total']
        agg_range_result = {}
        for (key, value_list) in ex_query_params_dict.iteritems():
            value = value_list[0]
            var_name, field_name = unbind_variable('ex_section_(?P<field_name>[\\d\\D]+)', 'field_name', key)
            agg_range_result_key = field_name + '_range'
            if agg_range_result_key not in es_result['aggregations']:
                continue
            optimize = bool(
                unbind_variable(extend_parser.section_regex_optimize, 'optimize', value)[1] or config.get_value(
                    'consts/global/algorithm/optimize'))
            section_num = int(unbind_variable(extend_parser.section_regex_size, 'size', value)[1] or '0')
            agg_range_list = equal_section_partitions.merge_child_sections(
                es_result['aggregations'][agg_range_result_key]['buckets'], total_doc_count, section_num, optimize)
            agg_range_result[field_name + '_section'] = agg_range_list
        debug_log.print_log('parse_es_agg_range_result spends {0}', (time.time() - start_time))
        return agg_range_result

    def __parse_nomal_agg_result(self, agg_result_dict, field):
        """
        解析普通的字段聚合结果
        :param agg_result_dict:
        :param field:
        :return:
        """
        return agg_result_dict[field]['buckets'] if field in agg_result_dict else []

    def __parse_prop_agg_result(self, agg_result_dict, field, is_last_cat=False, ignore_cat=False):
        """
        解析扩展属性聚合结果
        :param agg_result_dict:
        :param field:
        :param is_last_cat:
        :param ignore_cat 是否忽略cat,即不管是否是叶子类目都返回props聚合
        :return:
        """
        if field not in agg_result_dict:
            return []
        if not ignore_cat and not is_last_cat:
            return []

        prop_field_list = agg_result_dict[field]['name']['buckets']
        return map(
            lambda item: {'key': item['key'], 'doc_count': item['doc_count'], 'childs': item['value']['buckets']},
            prop_field_list)

    def __parse_cats_agg_result(self, agg_result_dict, field, is_last_cart):
        """
        解析路径聚合结果，可能会聚合很多层
        :param agg_result_dict:
        :param field:
        :param is_last_cart:
        :return:
        """
        if field not in agg_result_dict:
            return []

        prop_field_list = agg_result_dict[field]['name']['buckets']
        return map(lambda item: self.__get_cats_agg_result_item(item), prop_field_list)

    def __parse_key_value_agg_result(self, agg_result_dict, field):
        """
        解析key_vlaue聚合结果，
        :param agg_result_dict:
        :param field:
        :return:
        """
        if field not in agg_result_dict:
            return []
        key_value_list = agg_result_dict[field]['buckets']
        agg_result = {}
        for item in key_value_list:
            temp_strs = item['key'].split('*##*')
            if len(temp_strs) < 2:
                continue
            value_item = {'key': temp_strs[1], 'doc_count': item['doc_count']}
            agg_result.setdefault(temp_strs[0], [])
            agg_result[temp_strs[0]].append(value_item)
        agg_result_item_list = []
        for key_str, agg_item_list in agg_result.iteritems():
            total_doc_count = sum(map(lambda agg_item: agg_item['doc_count'], agg_item_list))
            agg_result_item_list.append({'key': key_str, 'doc_count': total_doc_count, 'value': agg_item_list})
        return sorted(agg_result_item_list, key=lambda item: item['doc_count'], reverse=True)

    def __get_cats_agg_result_item(self, result_item):
        if 'childs' not in result_item:
            return result_item
        value_list = map(lambda item: self.__get_cats_agg_result_item(item), result_item['childs']['name']['buckets'])
        return {'key': result_item['key'], 'doc_count': result_item['doc_count'], 'childs': value_list}

    def __is_last_cat(self, agg_result_dict):
        """
        判断是否是最后一层，如果是最后一层需要返回prop属性聚合信息
        :param agg_result_dict:
        :param field:
        :return:
        """
        if 'cats' not in agg_result_dict:
            return False
        cats_agg_result = agg_result_dict['cats']['name']['buckets']
        wheel_cats_agg_result = agg_result_dict['wheel_cats']['name']['buckets']
        return self.__get_cats_level(cats_agg_result) == self.__get_cats_level(wheel_cats_agg_result)

    def __is_ignore_cat(self, args):
        """
        判断是否需要忽略类目层次
        :param args:
        :return:
        """
        if not args or 'props_agg_ignore_cat' not in args:
            return False
        return args.get('props_agg_ignore_cat').lower().strip() == 'true'

    def __get_cats_level(self, cats_agg_list):
        """
        获取路径最大层次
        :param cats_agg_list:
        :return:
        """
        if len(cats_agg_list) == 0 or 'childs' not in cats_agg_list[0]:
            return 0
        return 1 + self.__get_cats_level(cats_agg_list[0]['childs']['name']['buckets'])


class EsCommonSuggestManager(object):
    """
    通用的搜索引擎suggester
    """
    connection_pool = EsConnectionFactory

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        查询通用的suggest数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        admin_id = parse_fields.get('adminId')
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = self._get_suggest_dsl(args, admin_id)
        app_log.info('Get common suggest qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     qdsl)
        try:
            es_result = es_connection.suggest(index=index_name, body=qdsl)
            es_result = es_result['completion_suggest'][0]['options']
            result = []
            for value in es_result:
                result.append(value['text'])
            return result
        except Exception as e:
            app_log.error('Get common suggest error, index={0} , type={1} , args={2}', e, index_name, doc_type, args)
            return []

    def save(self, es_config, index_name, doc_type, product, parse_fields, timestamp, redo=False):
        """
        把字段经过处理之后保存到索引中
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields:
        :param timestamp:
        :param redo:
        :return:
        """
        admin_id = parse_fields.get('adminId')
        word_list = product
        if not isinstance(word_list, (list, tuple)):
            word_list = [word_list]

        app_log.info('Save common suggest words, {}, {}'.format(word_list, index_name))
        body = []
        for word in word_list:
            weight = None
            if isinstance(word, dict):
                weight = word.get('weight')
                word = word['word']

            if not isinstance(word, (str, unicode)):
                raise ValueError('{} is not string type.'.format(word))

            input_value = pingyin_utils.get_pingyin_combination(word) + [word]
            dsl = {
                'id': word.encode('raw_unicode_escape'),
                'word': word,
                'suggest': {
                    'input': input_value,
                    'output': word
                }
            }

            if weight is not None:
                dsl['suggest']['weight'] = weight
            if admin_id is not None:
                dsl['id'] = admin_id + '-' + dsl['id']
                dsl['suggest']['context'] = {'adminId': admin_id}
            body.append(dsl)
        es_adapter.batch_create(es_config, body)

    def delete(self, es_config, index_name, doc_type, product, parse_fields, timestamp, redo):
        """
        删除指定的关键词
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields:
        :param timestamp:
        :param redo:
        :return:
        """
        word = product['word']
        word = word.encode('raw_unicode_escape')
        admin_id = parse_fields.get('adminId')
        if admin_id is not None:
            word = admin_id + '-' + word
        app_log.info('Delete common suggest word, {}, {}'.format(word, index_name))
        es_adapter.batch_delete_by_ids(es_config, word)

    def _get_suggest_dsl(self, query_params, admin_id):
        """
        根据待查询的字段生成查询的dsl
        :param query_params:
        :param admin_id
        :return:
        """
        suggest_qdl = {
            "completion_suggest": {
                "text": "",
                "completion": {
                    "field": "suggest",
                    "size": 10
                }
            }
        }
        word = get_dict_value(query_params, 'q')
        suggest_qdl['completion_suggest']['text'] = word
        suggest_size = get_dict_value(query_params, 'size', DEFAULT_VALUE['suggest_size']['default'],
                                      DEFAULT_VALUE['suggest_size']['min'], DEFAULT_VALUE['suggest_size']['max'])
        suggest_qdl['completion_suggest']['completion']['size'] = int(suggest_size)

        if admin_id is not None:
            suggest_qdl['completion_suggest']['completion']['context'] = {'adminId': admin_id}
        return suggest_qdl


class EsSuggestManager(object):
    connection_pool = EsConnectionFactory

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        查询Suggest数据
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_suggest_qdl(index_name, doc_type, args, parse_fields)
        app_log.info('Get suggest qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     qdsl)
        try:
            es_result = es_connection.suggest(index=index_name, body=qdsl)

            result = self.parse_es_result(es_result, args)
            return result
        except Exception as e:
            app_log.error('Get suggest error, index={0} , type={1} , args={2}', e, index_name, doc_type, args)
            return []

    def parse_es_result(self, es_result, args):
        """
        解析ES返回结果
        :param es_result:
        :param args:
        :return:
        """
        if 'completion_suggest' not in es_result or not es_result['completion_suggest'] or 'options' not in \
                es_result['completion_suggest'][0]:
            return []
        tag_name = args.get('tag') or 'default'
        default_value = config.get_value('/consts/global/query_size')
        suggest_size = get_dict_value(args, 'size', default_value['suggest_size']['default'],
                                      default_value['suggest_size']['min'], default_value['suggest_size']['max'])
        suggest_size = int(suggest_size)

        options = es_result['completion_suggest'][0]['options']
        suggest_term_list = filter(lambda suggest_term: suggest_term['doc_count'], map(
            lambda suggest_res: {'key': suggest_res['text'], 'doc_count': suggest_res['payload']['hits'][tag_name]},
            options))
        return suggest_term_list if len(suggest_term_list) <= suggest_size else suggest_term_list[:suggest_size]


class YxdShopSuggestManager(object):
    connection_pool = EsConnectionFactory

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        对云小店的店铺名称进行搜索提示
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_yxd_suggest_qdl(args, parse_fields)
        app_log.info('Get suggest qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args, qdsl)
        try:
            es_result = es_connection.suggest(index=index_name, body=qdsl)
            es_result = es_result['completion_suggest'][0]['options']
            result = []
            for value in es_result:
                result.append(value['text'])
            return result
        except Exception as e:
            app_log.error('Get suggest error, index={0} , type={1} , args={2}', e, index_name, doc_type, args)
            return []


class EsSearchManager(object):
    connection_pool = EsConnectionFactory

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        查询聚合数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        start_time = time.time()
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_search_qdl(es_config, index_name, doc_type, args, parse_fields, es_connection)
        qdsl_end_time = time.time()
        app_log.info('Get search dsl finish, spend time {4},  index={0} , type={1} , args={2}, dsl={3}', index_name,
                     doc_type, args, qdsl, qdsl_end_time - start_time)
        es_search_params = get_es_search_params(es_config, index_name, doc_type, args, parse_fields)
        if args.get('scene') == 'spu_aggs':
            # 根据sku聚合搜索spu场景
            result, es_result = spu_search_scene.get_spu_by_sku(qdsl, es_config, args, parse_fields, es_search_params)
        else:
            es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl,
                                             **es_search_params)
            es_end_time = time.time()
            app_log.info('Elasticsearch search index={0} , type={1} , spend time {2}', index_name, doc_type,
                         es_end_time - qdsl_end_time)
            result = self.parse_es_result(es_result, args)
            app_log.info('Parse elasticsearch search result index={0} , type={1} , spend time {2}', index_name,
                         doc_type, time.time() - es_end_time)
        range_result = Aggregation.objects.get_agg_range_result(es_config, index_name, doc_type, args, es_result,
                                                                qdsl)
        if range_result:
            result = deep_merge(result, {'aggregations': range_result})
        debug_log.print_log('EsSearchManager get return is omitted')
        return result

    def parse_es_result(self, es_result, args):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        product_result = Product.objects.parse_es_result(es_result, args)
        agg_result = Aggregation.objects.parse_es_result(es_result, args)
        search_result = {}
        if product_result:
            search_result['products'] = product_result
        if agg_result:
            search_result['aggregations'] = agg_result
        return search_result


class SearchPlatformDocManager(EsSearchManager):
    """
    搜索平台文档管理接口
    """

    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        通过ES查询文档数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        if not es_connection:
            raise EsConnectionError()

        qdsl = qdsl_parser.get_search_qdl(es_config, index_name, doc_type, args, parse_fields, es_connection,
                                          ignore_default_agg=True)
        app_log.info('Get doc qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     qdsl)

        if args.get('ex_body_type') == 'scroll':
            qdsl.pop('aggs')
            es_result = self.__scroll_search(qdsl, es_config, index_name, doc_type, args, parse_fields)
        else:
            es_search_params = get_es_search_params(es_config, index_name, doc_type, args, parse_fields)
            es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl,
                                             **es_search_params)
        result = self.parse_es_result(es_result, args)
        debug_log.print_log('SearchPlatformDocManager get return size is {0}',
                            result['total'] if 'total' in result else 'omitted')
        return result

    def get_dsl(self, es_config, index_name=None, doc_type=None, args=None, parse_fields=None, es_connection=None):
        """
        获取查询DSL
        :param es_config
        :param index_name:
        :param doc_type:
        :param args:
        :param es_connection:
        :return:
        """
        return qdsl_parser.get_product_query_qdsl(es_config, index_name, doc_type, args, parse_fields, es_connection)

    def parse_es_result(self, es_result, args):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        product_result = Product.objects.parse_es_result(es_result, args)
        agg_result = Aggregation.objects.parse_es_result(es_result, args)
        search_result = {}
        if product_result:
            search_result = product_result
        if agg_result:
            search_result['aggregations'] = agg_result
        return search_result

    def save(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        保存商品
        :param es_config:
        :param product:
        :return:
        """
        app_log.info(
            'Product save is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if not product:
            app_log.error('Product save input product is invalid')
            raise InvalidParamError()
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        _bulk_body = self._build_index_body(es_config, index_name, doc_type, product, parse_fields, timestamp)
        if not _bulk_body:
            return
        if redo:
            _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                     timestamp)
        es_bulk_result = es_connection.bulk(_bulk_body,
                                            params={'request_timeout': BATCH_REQUEST_TIMEOUT, 'timeout': BATCH_TIMEOUT})
        return es_adapter.get_es_bulk_result(es_bulk_result)

    def _filter_has_update_doc(self, es_config, index_name, doc_type, es_connection, _bulk_body, timestamp,
                               es_op_name='index'):
        """
        过滤掉已经被修改的文档记录
        :param es_config:
        :param index_name:
        :param doc_type:
        :param es_connection:
        :param _bulk_body:
        :param timestamp
        :param es_op_name es bulk操作类型， index, update, delete
        :return:
        """
        doc_id_template = 'generate' if 'id' not in es_config else es_config['id']
        if doc_id_template == 'generate':
            return _bulk_body
        doc_id_list = []
        es_exist_doc_list = []
        for bulk_body_item in _bulk_body:
            if es_op_name in bulk_body_item and isinstance(bulk_body_item[es_op_name], dict) \
                    and '_id' in bulk_body_item[es_op_name]:
                doc_id_list.append(bulk_body_item[es_op_name]['_id'])
        if doc_id_list:
            ids_query_dsl = {'query': {'ids': {'values': doc_id_list}}}
            search_result = es_connection.search(index=index_name, doc_type=doc_type, body=ids_query_dsl)
            es_exist_doc_list = map(lambda search_result_item:
                                    {'id': search_result_item['_id'],
                                     '_update_time': search_result_item['_source']['_update_time']
                                     if '_update_time' in search_result_item['_source'] else 0},
                                    search_result['hits']['hits'])
        no_need_operate_doc_ids = map(lambda es_doc: es_doc['id'],
                                      filter(lambda es_doc: es_doc['_update_time'] >= timestamp, es_exist_doc_list))
        if no_need_operate_doc_ids:
            _temp_bulk_item_list = []
            step = 1 if es_op_name == 'delete' else 2
            for index in xrange(0, len(_bulk_body), step):
                bulk_body_item = _bulk_body[index]
                if es_op_name in bulk_body_item and bulk_body_item[es_op_name]['_id'] not in no_need_operate_doc_ids:
                    _temp_bulk_item_list.append(bulk_body_item)
                    if step == 2:
                        _temp_bulk_item_list.append(_bulk_body[index] + 1)
            _bulk_body = _temp_bulk_item_list
        return _bulk_body

    def _build_index_body(self, es_config, index_name, doc_type, product, parse_fields, timestamp):
        """
        创建批量index数据结构
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :return:
        """
        _bulk_body = []
        doc_id_template = 'generate' if 'id' not in es_config else es_config['id']
        if not isinstance(product, (list, tuple, set)):
            batch_product_list = [product]
        else:
            batch_product_list = product
        self._add_private_field(es_config, batch_product_list, parse_fields)
        for item_product in batch_product_list:
            es_index_info = {"index": {"_index": index_name, "_type": doc_type}} if doc_id_template == 'generate' \
                else {
                "index": {"_index": index_name, "_type": doc_type,
                          "_id": bind_variable(es_config['id'], item_product)}}
            item_product['_update_time'] = timestamp
            _bulk_body.append(es_index_info)
            _bulk_body.append(item_product)
        return _bulk_body

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
        if 'adminId' not in param or not param['adminId']:
            return
        admin_id = param['adminId']
        for item in data_list:
            item['_adminId'] = admin_id

    def update(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        更新商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :return:
        """
        app_log.info(
            'Search update is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if not product:
            app_log.error('Search update input product is invalid')
            raise InvalidParamError()
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        if parse_fields and 'id' in parse_fields and parse_fields['id']:
            doc_id = parse_fields['id']
            _bulk_body = self._build_update_body(es_config, index_name, doc_type, product, parse_fields, timestamp,
                                                 doc_id)
        else:
            _bulk_body = self._build_update_body(es_config, index_name, doc_type, product, parse_fields, timestamp)
        if redo:
            _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                     timestamp, 'update')
        es_bulk_result = es_connection.bulk(_bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                'timeout': BATCH_TIMEOUT})
        return es_adapter.get_es_bulk_result(es_bulk_result)

    def _build_update_body(self, es_config, index_name, doc_type, product, parse_fields, timestamp, doc_id=None):
        """
        构造ES 批量update结构
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :param doc_id
        :return:
        """
        _bulk_body = []
        if not isinstance(product, (list, tuple, set)):
            batch_product_list = [product]
        else:
            batch_product_list = product
        self._add_private_field(es_config, batch_product_list, parse_fields)
        for item_product in batch_product_list:
            item_product['_update_time'] = timestamp
            es_update_info = {
                "update": {"_index": index_name, "_type": doc_type,
                           "_id": bind_variable(es_config['id'], item_product) if not doc_id else doc_id}}
            _bulk_body.append(es_update_info)
            _bulk_body.append({'doc': item_product})
        return _bulk_body

    def delete(self, es_config, index_name, doc_type, product, parse_fields=None, timestamp=None, redo=False):
        """
        删除商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields
        :param timestamp
        :param redo
        :return:
        """

        def parse_delete_doc_ids():
            if parse_fields and 'id' in parse_fields and parse_fields['id']:
                doc_id = parse_fields['id']
            elif 'doc_id' in product:
                doc_id = product['doc_id']
            else:
                if isinstance(product, (list, tuple, set)):
                    doc_id = map(lambda item: bind_variable(es_config['id'], item), product)
                else:
                    doc_id = bind_variable(es_config['id'], product)
            if not isinstance(doc_id, (list, tuple, set)):
                _doc_id_list = doc_id.split(',')
            else:
                _doc_id_list = doc_id
            return _doc_id_list

        app_log.info(
            'Search delete is called index_name={0} , doc_type={1} , product={2} , parse_fields={3} , '
            'timestamp={4} , redo={5}',
            index_name, doc_type, product, parse_fields, timestamp, redo)
        if product is None:
            app_log.error('Search delete input product is invalid')
            raise InvalidParamError()
        if product.get('ex_body_type') == 'scroll':
            return self.__delete_scroll_cache(es_config, index_name, doc_type, product, parse_fields)
        else:
            es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
            if not es_connection:
                raise EsConnectionError()
            doc_id_list = parse_delete_doc_ids()
            _bulk_body = map(lambda _doc_id: {'delete': {'_index': index_name, '_type': doc_type, '_id': _doc_id}},
                             doc_id_list)
            if redo:
                _bulk_body = self._filter_has_update_doc(es_config, index_name, doc_type, es_connection, _bulk_body,
                                                         timestamp, 'delete')
            es_bulk_result = es_connection.bulk(_bulk_body, params={'request_timeout': BATCH_REQUEST_TIMEOUT,
                                                                    'timeout': BATCH_TIMEOUT})
            return es_adapter.get_es_bulk_result(es_bulk_result)

    def __scroll_search(self, qdsl, es_config, index_name, doc_type, args, parse_fields=None):
        """
        scroll查询
        第一次执行参数：ex_body_type=scroll&scroll_time=1m&search_type=scan
        ex_body_type参数 scroll值表示使能scroll搜索，必填
        scroll_time表示缓存保存时间，可以不填，默认为系统参数/consts/query/scroll_time
        search_type 为scan，如果配置了该参数，搜索时不会进行排序，会大大加快执行速度
        返回结果会带有"_scroll_id"字段，下一次请求是需要带上该字段
        后续执行参数 ex_body_type=scroll &_scroll_id=wierowirowirowiroiwr & scroll_time=1m
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        scroll_time = args.get('scroll_time') or config.get_value('/consts/query/scroll_time')
        return es_adapter.scroll(scroll_time, qdsl, search_type=args.get('search_type'),
                                 scroll_id=args.get('_scroll_id'), index=index_name, doc_type=doc_type,
                                 host=es_config.get('host'))

    def __scan_search(self, qdsl, es_config, index_name, doc_type, args, parse_fields=None):
        """
        scan查询，查询所有符合条件的数据，不进行排序
        :param qdsl:
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        scroll_time = args.get('scroll_time') or config.get_value('/consts/query/scroll_time')
        iter_es_scan_result = es_adapter.scan(scroll_time, qdsl, index=index_name, doc_type=doc_type,
                                              host=es_config.get('host'))
        es_result = {'hits': {'hits': []}}
        iter_es_scan_result = list(iter_es_scan_result)
        for item_scan_result in iter_es_scan_result:
            if item_scan_result:
                es_result['hits']['hits'].append(item_scan_result)
        es_result['hits']['total'] = len(iter_es_scan_result)
        return es_result

    def __delete_scroll_cache(self, es_config, index_name, doc_type, product, parse_fields):
        """
        删除scroll缓存
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :param parse_fields:
        :return:
        """
        return es_adapter.delete_scroll(product.get('_scroll_id'), index=index_name, doc_type=doc_type,
                                        host=es_config.get('host'))


class StatsManager(object):
    """
    基本性能统计，不支持按照时间间隔统计，只输出所有性能任务采集数据
    """

    def get(self, es_config=None, index_name=None, doc_type=None, args=None, parse_fields=None):
        task_name = parse_fields.get('task_name')

        measure_obj_key = parse_fields.get('index')
        if args.get('scene') == 'product':
            # 如果是商店的性能统计，那么index传递的是用户的adminId
            es_config = es_adapter.get_product_es_cfg(admin_id=measure_obj_key)
            measure_obj_key = es_config['index']

        # 如果结束时间为空，那么结束时间取当前时间
        # end_date = long(args.get('enddate').strip()) if args.get('enddate') else get_time_by_mill()
        end_date = args.get('enddate').strip() if args.get('enddate') else 'now'
        # 如果开始时间为空，那么取结束时间的前30天数据
        # start_date = long(args.get('startdate').strip()) if args.get('startdate') else end_date - 30 * 24 * 3600 * 1000
        start_date = args.get('startdate').strip() if args.get('startdate') else 'now-3M/M'
        date_format = args.get('dateformat')

        metrics = args.get('metrics').split(',') if args.get('metrics') else []
        es_host = config.get_value('/measure/es/task/host') or get_default_es_host()
        search_dsl = self._get_query_dsl(start_date, end_date, metrics, measure_obj_key, date_format)
        sum_num = 0
        measure_result = {}
        for metric_name in metrics:
            measure_result[metric_name] = []
        while True:
            es_query_result = es_adapter.query_docs(search_dsl, es_host, settings.MEASUERE_ALIAS, task_name)
            cur_num = len(es_query_result['hits']['hits'])
            sum_num += cur_num
            for item in es_query_result['hits']['hits']:
                time_key = item['fields']['@collect_time']
                for metric_name in item['fields']:
                    if metric_name == '@collect_time':
                        continue
                    metric_values = item['fields'][metric_name]
                    if metric_values:
                        measure_result[metric_name].append(
                            {'key': time_key[0] if time_key else time_key, 'equal': metric_values[0]})

            if cur_num < 1000:
                break
            search_dsl['from'] = sum_num
        return measure_result

    def _get_query_dsl(self, start_date, end_date, metrics, measure_obj_key, date_format=None):
        date_range_dsl = {"range": {"@collect_time": {"gte": start_date, "lte": end_date}}}
        if date_format:
            date_range_dsl['format'] = date_format
        return {"query": {"bool": {"must": [date_range_dsl,
                                            {"term": {"@obj_key": measure_obj_key}}]}},
                "size": 1000,
                "from": 0,
                "sort": [{"@collect_time": "asc"}],
                "fields": ["@collect_time"] + metrics}


class ExStatsManager(object):
    """
    扩展性能统计，支持按照一定的时间间隔汇总，只支持周期为period方式的性能采集任务
    """
    connection_pool = EsConnectionFactory

    def get(self, es_config=None, index_name=None, doc_type=None, args=None, parse_fields=None):
        """
        获取性能任务数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :param parse_fields:
        :return:
        """
        task_name = parse_fields.get('task_name')
        period_cfg = measure_unit_helper.get_task_period_cfg(task_name)
        period_type = period_cfg.get('type')
        if period_type != 'interval':
            raise InvalidParamError('The task({0}) period type is not \"period\"'.format(task_name))

        measure_obj_key = parse_fields.get('index')
        if args.get('scene') == 'product':
            # 如果是商店的性能统计，那么index传递的是用户的adminId
            es_config = es_adapter.get_product_es_cfg(admin_id=measure_obj_key)
            measure_obj_key = es_config['index']

        # 如果结束时间为空，那么结束时间取当前时间
        # end_date = long(args.get('enddate').strip()) if args.get('enddate') else get_time_by_mill()
        end_date = args.get('enddate').strip() if args.get('enddate') else 'now'
        # 如果开始时间为空，那么取结束时间的前30天数据
        # start_date = long(args.get('startdate').strip()) if args.get('startdate') else end_date - 30 * 24 * 3600 * 1000
        start_date = args.get('startdate').strip() if args.get('startdate') else 'now-3M/M'

        # 如果不指定interval，那么直接返回单点每次采集数据
        interval = args.get('interval') if args.get('interval') else 'day'
        # 性能任务多个周期结果的计算方式，支持sum avg  max  min sum_of_squares variance std_deviation
        calculations = args.get('calculations').split(',') if args.get('calculations') else ['sum']
        calculations = filter(lambda calculation: calculation.strip(), calculations)

        metrics = args.get('metrics').split(',') if args.get('metrics') else []
        metric_dict = OrderedDict()
        for metric in metrics:
            if not metric:
                continue
            temp_strs = metric.strip().split(':')
            if len(temp_strs) == 1:
                # 表明该测量指标没有指定计算方式，采用默认计算方式
                metric_dict[temp_strs[0]] = calculations
            else:
                cur_calculation_list = temp_strs[1].split('|')
                metric_dict[temp_strs[0]] = cur_calculation_list

        # 如果指定了周期间隔，周期间隔必须为性能任务周期的整数倍，否则采集的数据会有问题
        msearch_body_list = chain(*map(
            lambda metric: self._get_agg_dsl(start_date, end_date, interval, metric, measure_obj_key),
            metric_dict.iterkeys()))
        es_host = config.get_value('/measure/es/task/host') or get_default_es_host()
        es_msearch_result = es_adapter.multi_search(msearch_body_list, es_host, settings.MEASUERE_ALIAS, task_name)
        measure_result = {}
        metric_name_list = list(metric_dict.iterkeys())
        for es_result in es_msearch_result['responses']:
            metric_stats_result_list = es_result['aggregations']['start_end_date']['interval']['buckets']
            metric_name = metric_name_list.pop(0)
            metric_calculations = metric_dict[metric_name]
            measure_result[metric_name] = []
            for metric_stats_result in metric_stats_result_list:
                filter_metric_stats_result = {'key': metric_stats_result['key_as_string']}
                for calculation_key in metric_stats_result['ex_stats']:
                    if calculation_key in metric_calculations:
                        filter_metric_stats_result[calculation_key] = metric_stats_result['ex_stats'][calculation_key]
                measure_result[metric_name].append(filter_metric_stats_result)
        return measure_result

    def _get_agg_dsl(self, start_date, end_date, interval, metric, measure_obj_key):
        """
        获取查询DSL
        :param start_date:
        :param end_date:
        :param interval:
        :param metric:
        :return:
        """
        return {}, {
            "query": {"term": {"@obj_key": measure_obj_key}},
            "aggs": {"start_end_date": {
                "filter": {"range": {"@collect_time": {"gte": start_date, "lte": end_date}}},
                "aggs": {"interval": {"date_histogram": {"field": "@collect_time", "interval": interval},
                                      "aggs": {"ex_stats": {"extended_stats": {"field": metric}}}}}}},
            "size": 0}


class ExSuggestManager(object):
    """
    关键词纠错操作接口
    """
    connection_pool = EsConnectionFactory

    def get(self, es_config=None, index_name=None, doc_type=None, args=None, parse_fields=None):
        if parse_fields.get('scene') == 'correction':
            query_str = args.get('q')
            if not query_str:
                raise InvalidParamError('Query string cannot be null')
            parse_fields = dict(es_config, **parse_fields) if parse_fields else es_config
            size_cfg = config.get_value('/consts/global/query_size/like_str_size')
            size_num = get_dict_value(args, 'size', size_cfg['default'], size_cfg['min'], size_cfg['max'], int)
            parse_fields['size'] = size_num
            return like_str_algorithm.get_like_query_string(query_str, parse_fields)
        elif parse_fields.get('scene') == 'suggestion':
            return Suggest.objects.get(es_config, index_name, doc_type, args, parse_fields)
        raise InvalidParamError('Scene is invalid {0}'.format(parse_fields.get('scene')))


class RecommendationManager(object):
    """
    商品推荐接口
    """
    connection_pool = EsConnectionFactory

    def get(self, es_config=None, index_name=None, doc_type=None, args=None, parse_fields=None):
        if parse_fields.get('scene') == 'content':
            # 基于内容的推荐
            return self.get_recommend_products_by_comment(es_config, args)

        raise InvalidParamError('Scene is invalid {0}'.format(parse_fields.get('scene')))

    def get_recommend_products_by_comment(self, es_config, args):
        """
        根据商品内容推荐商品
        :param es_config:
        :param args:
        :return:
        """
        if args.get('ids'):
            tag = args.get('tag') or 'b2c'
            products = self.__query_product_by_ids(es_config, args.get('ids'))
            product_cat_path_list = list(
                set(filter(lambda item: item, map(lambda _product: get_cats_path(_product, tag), products))))
            product_type_dict, range_dict = self.__query_product_info_by_cat_path(es_config, product_cat_path_list)
            recommend_product_list = content_recom.recommend_products_by_cosine(products, product_type_dict, args,
                                                                                range_dict, tag)
            return recommend_product_list

        raise InvalidParamError('The request parameter ids cannot be null')

    def __query_product_by_ids(self, es_config, ids):
        """
        根据商品ID查询商品
        :param es_config:
        :param ids:
        :return:
        """
        ids_query_dsl = {"query": {"ids": {"values": ids.split(',')}}}
        es_search_result = es_adapter.query_docs(ids_query_dsl, es_config['host'], es_config['index'],
                                                 es_config['type'])
        return map(lambda es_result_item: es_result_item['_source'], es_search_result['hits']['hits'])

    def __query_product_info_by_cat_path(self, es_config, cat_path_list):
        """
        根据商品cat path查询商品和统计信息
        :param es_config:
        :param cat_path_list:
        :return:
        """
        msearch_body = []
        range_dsl = self.__get_range_dsl()
        for cat_path_str in cat_path_list:
            cat_path = cat_path_str.split(',')
            cat_path_item_dsl = qdsl_parser.get_catpath_query_qdl(len(cat_path), cat_path)
            query_dsl = {"query": {"bool": {"must": [cat_path_item_dsl]}}, "size": config.get_value(
                '/consts/global/algorithm/content_based_recom/recommend/type_query_size')}
            if range_dsl:
                query_dsl = deep_merge(query_dsl, range_dsl)
            msearch_body.extend(({}, query_dsl))
        es_search_result = es_adapter.multi_search(msearch_body, es_config['host'], es_config['index'],
                                                   es_config['type'])
        product_type_dict = {}
        range_dict = {}
        index = 0
        for response in es_search_result['responses']:
            type_key = cat_path_list[index]
            if response['hits']['hits']:
                product_type_dict[type_key] = map(lambda es_result_item: es_result_item['_source'],
                                                  response['hits']['hits'])
                if 'aggregations' in response:
                    range_dict[type_key] = {}
                    for aggs_key in response['aggregations']:
                        range_dict[type_key][aggs_key] = response['aggregations'][aggs_key]
            index += 1
        return product_type_dict, range_dict

    def __get_range_dsl(self):
        """
        获取范围查询DSL
        :return:
        """
        product_vector_cfg = config.get_value('/consts/global/algorithm/content_based_recom/vectors')
        agg_dsl = {}
        for item in product_vector_cfg:
            if product_vector_cfg[item].get('type') == 'range':
                if 'aggs' not in agg_dsl:
                    agg_dsl['aggs'] = {}
                agg_dsl['aggs'][item] = {"stats": {"field": item}}
        return agg_dsl


class Product(EsModel):
    objects = EsProductManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class CommonSuggest(EsModel):
    objects = EsCommonSuggestManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Suggest(EsModel):
    objects = EsSuggestManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class YxdShopSuggest(EsModel):
    objects = YxdShopSuggestManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Aggregation(EsModel):
    objects = EsAggManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Search(EsModel):
    objects = EsSearchManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class SearchPlatformDoc(EsModel):
    objects = SearchPlatformDocManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Stats(EsModel):
    objects = StatsManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class ExStats(EsModel):
    objects = ExStatsManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class ExSuggest(EsModel):
    objects = ExSuggestManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Recommendation(EsModel):
    objects = RecommendationManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


if __name__ == '__main__':
    product = {
        "cats": [
            {
                "childs": [
                    {
                        "childs": [
                            {
                                "childs": [
                                    {
                                        "childs": [],
                                        "id": "102010021003",
                                        "name": "蜜饯干果"
                                    }
                                ],
                                "id": "10201002",
                                "name": "时尚零食"
                            }
                        ],
                        "id": "1020",
                        "name": "休闲食品"
                    }
                ],
                "id": "b2c",
                "name": "b2c"
            },
            {
                "childs": [
                    {
                        "childs": [
                            {
                                "childs": [
                                    {
                                        "childs": [],
                                        "id": "102110021003",
                                        "name": "蜜饯干果"
                                    }
                                ],
                                "id": "10211002",
                                "name": "时尚零食"
                            }
                        ],
                        "id": "1021",
                        "name": "休闲食品"
                    }
                ],
                "id": "b2b",
                "name": "b2b"
            }
        ],
    }

    print 'result: ' + get_cats_path(product, 'b2c')
