# -*- coding: utf-8 -*-
# Create your models here.
from collections import OrderedDict
from itertools import chain
import time

from algorithm.content_based_recom import content_recom
from algorithm.like_query_string import like_str_algorithm
from algorithm.section_partitions import equal_section_partitions
from common.adapter import es_adapter
from common.configs import config
from common.exceptions import InvalidParamError, EsConnectionError
from common.connections import EsConnectionFactory
from common.loggers import debug_log, query_log as app_log
from common.utils import unbind_variable, deep_merge, bind_variable, get_default_es_host, get_dict_value
from measure.measure_units import measure_unit_helper
from qdsl_parseres import qdsl_parser, extend_parser
from search_platform import settings
from service.search_scenes import spu_search_scene


__author__ = 'liuzhaoming'


class EsModel(object):
    field_config = {}

    def __init__(self, **args):
        self.__dict__.update([(key, value) for (key, value) in args.iteritems() if key in self.field_config])


class EsProductManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsProductManger.get::')
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
        qdsl = qdsl_parser.get_product_query_qdsl(index_name, doc_type, args, es_connection)
        qdsl_end_time = time.time()
        app_log.info('Get product dsl finish, spend time {4}, index={0} , type={1} , args={2}, dsl={3}', index_name,
                     doc_type, args, qdsl, qdsl_end_time - start_time)
        if args.get('scene') == 'spu_aggs':
            # 根据sku聚合搜索spu场景
            result, es_agg_result = spu_search_scene.get_spu_by_sku(qdsl, es_config, args)
        else:
            if args.get('ex_body_type') == 'scroll':
                es_result = self.__scroll_search(qdsl, es_config, index_name, doc_type, args, parse_fields)
            elif args.get('ex_body_type') == 'scan':
                es_result = self.__scan_search(qdsl, es_config, index_name, doc_type, args, parse_fields)
            else:
                es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
            es_end_time = time.time()
            app_log.info('Elasticsearch search index={0} , type={1} , spend time {2}', index_name, doc_type,
                         es_end_time - qdsl_end_time)
            result = self.parse_es_result(es_result, args)
            app_log.info('Parse elasticsearch search result index={0} , type={1} , spend time {2}', index_name,
                         doc_type, time.time() - es_end_time)
        debug_log.print_log('EsProductManager get return size is {0}',
                            result['total'] if 'total' in result else 'omitted')
        return result

    def save(self, es_config, index_name, doc_type, product, parse_fields=None):
        """
        保存商品
        :param es_config:
        :param product:
        :return:
        """
        app_log.info('Product save is called {0} , {1} , {2} , {3}', index_name, doc_type, product, parse_fields)
        if not product:
            app_log.error('Product save input product is invalid')
            raise InvalidParamError()

        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        doc_id = bind_variable(es_config['id'], product)
        es_connection.index(index=index_name, doc_type=doc_type, body=product, id=doc_id)
        return product

    def update(self, es_config, index_name, doc_type, product, parse_fields=None):
        """
        更新商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :return:
        """
        app_log.info('Product update is called {0} , {1} , {2} , {3}', index_name, doc_type, product, parse_fields)
        if not product:
            app_log.error('Product update input product is invalid')
            raise InvalidParamError()

        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=True)
        if not es_connection:
            raise EsConnectionError()
        if parse_fields and 'id' in parse_fields and parse_fields['id']:
            doc_id = parse_fields['id']
        else:
            doc_id = bind_variable(es_config['id'], product)
        es_connection.update(index=index_name, doc_type=doc_type, body={'doc': product}, id=doc_id)
        return product

    def delete(self, es_config, index_name, doc_type, product, parse_fields=None):
        """
        删除商品数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param product:
        :return:
        """
        app_log.info('Product delete is called {0} , {1} , {2} , {3}', index_name, doc_type, product, parse_fields)
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
            if parse_fields and 'id' in parse_fields and parse_fields['id']:
                doc_id = parse_fields['id']
            elif 'doc_id' in product:
                doc_id = product['doc_id']
            else:
                doc_id = bind_variable(es_config['id'], product)
            es_connection.delete(index=index_name, doc_type=doc_type, id=doc_id)

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
        qdsl = qdsl_parser.get_agg_qdl(index_name, doc_type, args, es_connection)
        app_log.info('Get agg dsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args, qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result)
        range_result = self.get_agg_range_result(es_config, index_name, doc_type, args, es_result, qdsl)
        if range_result:
            result = deep_merge(result, range_result)
        app_log.info('EsAggManager get return is {0}', result)
        return result

    def parse_es_result(self, es_result):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        result = {}
        if 'aggregations' not in es_result:
            return result

        agg_result = es_result['aggregations']
        is_last_cat = self.__is_last_cat(agg_result)
        for agg_key in agg_result:
            if agg_key == 'brand':
                result['brand'] = self.__parse_nomal_agg_result(agg_result, 'brand')
            elif agg_key == 'cats':
                result['cats'] = self.__parse_cats_agg_result(agg_result, 'cats', is_last_cat)
            elif agg_key == 'props':
                result['props'] = self.__parse_prop_agg_result(agg_result, 'props', is_last_cat)
            elif agg_key.startswith('ex_agg_'):
                if agg_key.endswith('.cats'):
                    result[agg_key] = self.__parse_cats_agg_result(agg_result, agg_key, is_last_cat)
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
        print 'get_agg_range_result spends {0}'.format(time.time() - start_time)
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
        print 'parse_es_agg_range_result spends {0}'.format(time.time() - start_time)
        return agg_range_result


    def __parse_nomal_agg_result(self, agg_result_dict, field):
        """
        解析普通的字段聚合结果
        :param agg_result_dict:
        :param field:
        :return:
        """
        return agg_result_dict[field]['buckets'] if field in agg_result_dict else []

    def __parse_prop_agg_result(self, agg_result_dict, field, is_last_cat=False):
        """
        解析扩展属性聚合结果
        :param agg_result_dict:
        :param field:
        :param is_last_cat:
        :return:
        """
        if field not in agg_result_dict or not is_last_cat:
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
        cats_agg_result = agg_result_dict['cats']['name']['buckets']
        wheel_cats_agg_result = agg_result_dict['wheel_cats']['name']['buckets']
        return self.__get_cats_level(cats_agg_result) == self.__get_cats_level(wheel_cats_agg_result)

    def __get_cats_level(self, cats_agg_list):
        """
        获取路径最大层次
        :param cats_agg_list:
        :return:
        """
        if len(cats_agg_list) == 0 or 'childs' not in cats_agg_list[0]:
            return 0
        return 1 + self.__get_cats_level(cats_agg_list[0]['childs']['name']['buckets'])


class EsSuggestManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsSuggestManger.get::')
    def get(self, es_config, index_name, doc_type, args, parse_fields=None):
        """
        查询Suggest数据
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        return []
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_suggest_qdl(index_name, doc_type, args)
        app_log.info('Get suggest qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     qdsl)
        es_result = es_connection.suggest(index=index_name, body=qdsl)

        result = self.parse_es_result(es_result, args)
        debug_log.print_log('EsSuggestManager get return is {0}', result)
        return result

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


class EsSearchManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsSearchManager.get::')
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
        qdsl = qdsl_parser.get_search_qdl(index_name, doc_type, args, es_connection)
        qdsl_end_time = time.time()
        app_log.info('Get search dsl finish, spend time {4},  index={0} , type={1} , args={2}, dsl={3}', index_name,
                     doc_type, args, qdsl, qdsl_end_time - start_time)
        if args.get('scene') == 'spu_aggs':
            # 根据sku聚合搜索spu场景
            result, es_result = spu_search_scene.get_spu_by_sku(qdsl, es_config, args)
        else:
            es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
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
        agg_result = Aggregation.objects.parse_es_result(es_result)
        search_result = {}
        if product_result:
            search_result['products'] = product_result
        if agg_result:
            search_result['aggregations'] = agg_result
        return search_result


class SearchPlatformDocManager(EsProductManager):
    """
    搜索平台文档管理接口
    """

    @debug_log.debug('SearchPlatformDocManager.get::')
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

        qdsl = qdsl_parser.get_product_query_qdsl(index_name, doc_type, args, es_connection)
        app_log.info('Get doc qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                     qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result, args)
        debug_log.print_log('SearchPlatformDocManager get return size is {0}',
                            result['total'] if 'total' in result else 'omitted')
        return result

    def get_dsl(self, index_name=None, doc_type=None, args=None, es_connection=None):
        """
        获取查询DSL
        :param index_name:
        :param doc_type:
        :param args:
        :param es_connection:
        :return:
        """
        return qdsl_parser.get_product_query_qdsl(index_name, doc_type, args, es_connection)


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
    扩展建议操作接口
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
            products = self.__query_product_by_ids(es_config, args.get('ids'))
            product_type_list = list(set(map(lambda product: product['type'], products)))
            product_type_dict, range_dict = self.__query_product_info_by_type(es_config, product_type_list)
            recommend_product_list = content_recom.recommend_products_by_cosine(products, product_type_dict, args,
                                                                                range_dict)
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

    def __query_product_info_by_type(self, es_config, type_list):
        """
        根据商品type查询商品和统计信息
        :param es_config:
        :param type_list:
        :return:
        """
        msearch_body = []
        range_dsl = self.__get_range_dsl()
        for product_type in type_list:
            type_query_dsl = {"query": {"term": {"type": product_type}}, "size": config.get_value(
                '/consts/global/algorithm/content_based_recom/recommend/type_query_size')}
            if range_dsl:
                type_query_dsl = deep_merge(type_query_dsl, range_dsl)
            msearch_body.extend(({}, type_query_dsl))
        es_search_result = es_adapter.multi_search(msearch_body, es_config['host'], es_config['index'],
                                                   es_config['type'])
        product_type_dict = {}
        range_dict = {}
        for response in es_search_result['responses']:
            if response['hits']['hits']:
                type_key = response['hits']['hits'][0]['_source']['type']
                product_type_dict[type_key] = map(lambda es_result_item: es_result_item['_source'],
                                                  response['hits']['hits'])
                if 'aggregations' in response:
                    range_dict[type_key] = {}
                    for aggs_key in response['aggregations']:
                        range_dict[type_key][aggs_key] = response['aggregations'][aggs_key]
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


class Suggest(EsModel):
    objects = EsSuggestManager()

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
    from elasticsearch import Elasticsearch

    es = Elasticsearch('http://172.19.65.66:9200')
    bulk_body = es._bulk_body([{'1': 22, '2': 'value2'}, {
        "name": "咖啡特浓即饮罐装24*180ML",
        "url": "http://www.esunny.com/bProductListDetailPreview.do?pid=2397&categoryid=312&brandid=106",
        "image": "http://www.esunny.com/photo/1393574402654.jpg",
        "barcode": "6917878028606",
        "relative_shops": [
            {
                "shop": "宇商网",
                "url": "http://www.esunny.com/bProductListDetailPreview.do?pid=2397&categoryid=312&brandid=106",
                "price": "￥"
            }
        ],
        "details": [
            {
                "商品规格": "24*180ML"
            }
        ],
        "id": "6917878028606"
    }])
    print bulk_body

