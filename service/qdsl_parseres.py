# -*- coding: utf-8 -*-
import json
from itertools import *

import re

from algorithm.section_partitions import equal_section_partitions
from common.configs import config
from common.connections import EsConnectionFactory
from common.exceptions import InvalidParamError
from common.scripts import python_invoker
from common.utils import singleton, get_dict_value, unbind_variable, deep_merge
from common.loggers import debug_log, query_log
from search_platform import settings
from common.adapter import es_adapter


__author__ = 'liuzhaoming'

DEFAULT_VALUE = config.get_value('/consts/global/query_size')


@singleton
class QdslParser(object):
    DEFAULT_AGG_SIZE = 1000

    suggest_qdl = {
        "completion_suggest": {
            "text": "",
            "completion": {
                "field": "suggest",
                "size": 10
            }
        }
    }

    def get_origin_agg_qdl(self):
        agg_size = config.get_value('/consts/global/agg_size') or self.DEFAULT_AGG_SIZE
        agg_dsl = {
            # "category": {"terms": {"field": "category"}
            # },
            "brand": {
                "terms": {
                    "field": "brand",
                    "size": agg_size
                }
            },
            "props": {
                "nested": {
                    "path": "props"
                },
                "aggs": {
                    "name": {
                        "terms": {
                            "field": "props.name",
                            "size": agg_size
                        },
                        "aggs": {
                            "value": {
                                "terms": {
                                    "field": "props.value",
                                    "size": agg_size
                                }
                            }
                        }
                    }
                }
            }
        }
        return agg_dsl


    @debug_log.debug('get_product_query_qdsl')
    def get_product_query_qdsl(self, index, type, query_params, es_connection=None):
        """
        获取商品查询DSL
        :param index:
        :param query_params:
        :return:
        """
        query_string = get_dict_value(query_params, 'q')
        from_num = get_dict_value(query_params, 'from', DEFAULT_VALUE['from']['default'], DEFAULT_VALUE['from']['min'],
                                  DEFAULT_VALUE['from']['max'], int)
        size_num = get_dict_value(query_params, 'size', DEFAULT_VALUE['size']['default'], DEFAULT_VALUE['size']['min'],
                                  DEFAULT_VALUE['size']['max'], int)
        category = get_dict_value(query_params, 'cat')
        basic_condition = get_dict_value(query_params, 'basic')
        prop_condition = get_dict_value(query_params, 'prop')
        sort = get_dict_value(query_params, 'sort')
        cats = get_dict_value(query_params, 'cats')
        # min_score = config.get_value('/consts/global/query_min_score') or 1.0

        must_bool_query_qdsl_list = list(
            chain(self.parse_query_string_condition(query_string, es_connection, query_params),
                  self.parse_cat_condition(category),
                  self.parse_basic_conditions(basic_condition),
                  self.parse_prop_conditions(prop_condition),
                  self.parse_catpath_query_condition(cats)))

        product_query_qdsl = reduce(lambda dict_1, dict_2: dict(dict_1, **dict_2),
                                    filter(lambda item: item,
                                           [{'query': {'bool': {'must': must_bool_query_qdsl_list}}},
                                            self.parse_page_param(from_num, size_num), self.parse_sort_params(sort)]))
        extend_query_qdsl = extend_parser.get_qdsl(query_params)
        return deep_merge(product_query_qdsl, extend_query_qdsl)

    def parse_query_string_condition(self, query_string, es_connection, query_params, field=None):
        """
        获取搜索框字符查询QDSL
        :param query_string:
        :return:
        """
        match_type = query_params.get('q_match_type')
        if not match_type == 'match_all' and not match_type == 'match_selected_fields':
            match_type = config.get_value('/consts/query/query_string/default')
        q_match_dsl = self._get_match_all_query_string_dsl(es_connection, field, query_string) \
            if match_type == 'match_all' else self._get_selected_fields_query_string_dsl(es_connection, query_string)
        score_dsl = self._get_score_query_string_dsl(es_connection, query_string)
        return q_match_dsl + score_dsl

    def _get_match_all_query_string_dsl(self, es_connection, field, query_string):
        """
        通过对_all 字段进行match查询的方式实现关键词搜索
        :param es_connection:
        :param field:
        :param query_string:
        :return:
        """
        analyzer = config.get_value('/consts/query/query_string/match_all/analyzer') or 'standard'
        default_index_name = config.get_value('/consts/query/default_index')
        field = config.get_value('/consts/query/query_string/match_all/fields') or field
        if query_string:
            analyze_token_list = es_adapter.query_text_analyze_result_without_filter(es_connection, analyzer,
                                                                                     query_string,
                                                                                     index=default_index_name)
            if analyze_token_list:
                return map(lambda analyze_token: {'match': {field: analyze_token}}, analyze_token_list)
        return [{'match': {field: query_string}} if query_string else {'match_all': {}}]

    def _get_selected_fields_query_string_dsl(self, es_connection, query_string):
        """
        通过对_all 字段进行match查询的方式实现关键词搜索
        :param es_connection:
        :param field:
        :param query_string:
        :return:
        """

        def _get_fields_match_dsl(_field_cfg, _word):
            normal_fields = _field_cfg.get('normal')
            normal_fields_match_dsl, nest_items_dsl_list = [], []
            if normal_fields:
                # normal_fields_match_dsl = [{'multi_match': {'query': _word, 'fields': normal_fields}}]
                for normal_field in normal_fields:
                    normal_fields_match_dsl.append({'match': {normal_field: _word}})

            nest_field_dict = _field_cfg.get('nest') or {}
            for nest_field_key in nest_field_dict:
                nest_field_item = nest_field_dict[nest_field_key]
                level = nest_field_item.get('level') or 0
                item_dsl = {'match': {nest_field_key: _word}}
                nest_item_dsl = self._get_nest_dsl(nest_field_item.get('path'), level, nest_field_item['field'],
                                                   item_dsl)
                nest_items_dsl_list.append(nest_item_dsl)
            if normal_fields_match_dsl or nest_items_dsl_list:
                fields_match_dsl = {
                    'bool': {'minimum_should_match': 1, 'should': normal_fields_match_dsl + nest_items_dsl_list}}
            else:
                fields_match_dsl = {}
            return fields_match_dsl


        analyzer = config.get_value('/consts/query/query_string/match_selected_fields/analyzer') or 'standard'
        default_index_name = config.get_value('/consts/query/default_index')
        fields_cfg = config.get_value('/consts/query/query_string/match_selected_fields/fields') or {}
        if query_string:
            analyze_token_list = es_adapter.query_text_analyze_result_without_filter(es_connection, analyzer,
                                                                                     query_string,
                                                                                     index=default_index_name)
            if analyze_token_list:
                return [{'bool': {'must': map(lambda analyze_token: _get_fields_match_dsl(fields_cfg, analyze_token),
                                              analyze_token_list)}}]
        return [{'match': {'_all': query_string}} if query_string else {'match_all': {}}]

    def _get_score_query_string_dsl(self, es_connection, query_string):
        """
        获取普通query_string查询作为得分项，主要是增加连续词的得分
        :param es_connection:
        :param query_string:
        :return:
        """
        if not query_string:
            return []
        boost = config.get_value('/consts/query/query_string/score/boost') or 1.0
        return [{'bool': {'minimum_should_match': 0, 'boost': boost, 'should': [{'match': {'_all': query_string}}]}}]

    def _get_nest_dsl(self, path, level, root_field, item_dsl):
        """
        获取嵌套查询的DSL
        :param path:
        :param level:
        :param root_field:
        :param item_dsl:
        :return:
        """
        if level == 0:
            return {'nested': {'path': root_field, 'query': item_dsl}}
        else:
            complete_path_list = [root_field] + list(repeat(path, level))
            return {'nested': {'path': '.'.join(complete_path_list),
                               'query': self._get_nest_dsl(path, level - 1, root_field, item_dsl)}}

    def parse_cat_condition(self, category):
        """
        获取category商品类目查询QDSL
        :param category:
        :return:
        """
        return [{'terms': {self.field_to_es_field('category'): category.strip().split(
            ',')}}] if category and category.strip() else []

    def parse_page_param(self, from_num, size_num):
        """
        生成分页相关qdsl
        :param from_num:
        :param size_num:
        :return:
        """
        return {'from': from_num, 'size': size_num}

    def parse_sort_params(self, sort):
        """
        生成排序QDSL
        :param sort:
        :return:
        """
        if not sort or not sort.strip():
            return None
        if 'script:' in sort or 'geodistance' in sort:
            sort_item_list = sort.split(';')
        elif ';' in sort:
            sort_item_list = sort.split(';')
        elif '_' in sort:
            sort_item_list = sort.split('_')
        else:
            sort_item_list = sort.split(';')
        return {'sort': [self.parse_sort_param_item(item) for item in sort_item_list if item]}


    def parse_sort_param_item(self, sort_item):
        """
        单个排序字段进行处理，字段和顺序用":"分割，price:1 示例表示：价格升序
        sort=price:1_stock:0
        Sort=price:1;stock:0
        Sort=price:1;stock:order(0)
        Sort=price:1; script:script(),type(),order() ; geodistance:location(48.8, 2.35),unit(km),order(0)
        :param sort_item:
        :return:
        """
        sort_field_id, sort_seq_id = sort_item.strip().split(':', 1)
        order = unbind_variable('order\\((?P<order>[\\d\\D]+?)\\)', 'order', sort_seq_id)[1] or sort_seq_id
        order = self.order_num_to_es_str(order)
        if sort_field_id == 'script':
            # 支持根据动态脚本排序
            script = unbind_variable('script\\((?P<script>[\\d\\D]+?)\\)', 'script', sort_seq_id)[1]
            script = extend_parser.format_script_str(script)
            script_type = unbind_variable('type\\((?P<type>[\\d\\D]+?)\\)', 'type', sort_seq_id)[1] or 'string'
            return {'_script': {'script': script, 'type': script_type, 'order': order}}
        elif sort_field_id == 'geodistance':
            # 支持根据距离排序
            location = unbind_variable('location\\((?P<location>[\\d\\D]+?)\\)', 'location', sort_seq_id)[1]
            unit = unbind_variable('unit\\((?P<unit>[\\d\\D]+?)\\)', 'unit', sort_seq_id)[1] or 'km'
            distance_type = unbind_variable('distancetype\\((?P<distancetype>[\\d\\D]+?)\\)', 'distancetype',
                                            sort_seq_id)[1] or 'sloppy_arc'
            mode = unbind_variable('mode\\((?P<mode>[\\d\\D]+?)\\)', 'mode', sort_seq_id)[1]
            return {'_geo_distance': {'pin.location': location, 'unit': unit, 'distance_type': distance_type,
                                      'mode': mode}} if mode else {
                '_geo_distance': {'pin.location': location, 'unit': unit, 'distance_type': distance_type}}
        return {self.global_id_to_field(sort_field_id): order}


    def parse_basic_conditions(self, basic_conditions):
        """
        获取基础查询条件QDSL
        :param basic_condition:
        :return:
        """
        return chain(*map(lambda condition_item: self.parse_basic_condition_item(condition_item),
                          basic_conditions.split('_'))) if basic_conditions else []

    def parse_basic_condition_item(self, basic_condition_item):
        """
        解析基本查询条件中的每个查询项,防止传过来的参数有问题，需要做容错性处理
        :param basic_condition_item:
        :return:
        """
        if not basic_condition_item or ':' not in basic_condition_item:
            return []
        key, value_str = basic_condition_item.strip().split(':', 1)
        values = value_str.strip().split(',')
        if key == 'brand':
            item_qdsl = [{'terms': {self.field_to_es_field('brand'): values}}]
        elif key == 'price':
            item_qdsl = map(
                lambda value_item: {'range': {self.field_to_es_field('price'): self.to_range_qdsl(value_item)}},
                values)
            item_qdsl = [{'bool': {'should': item_qdsl, 'minimum_should_match': 1}}]
        return item_qdsl

    def parse_prop_conditions(self, prop_conditions):
        """
        获取Prop扩展属性查询条件QDSL
        :param prop_conditions:
        :return:
        """
        return chain(*map(lambda condition_item: self.parse_prop_condition_item(condition_item),
                          prop_conditions.split('_'))) if prop_conditions else []

    def parse_prop_condition_item(self, prop_condition_item):
        """
        解析扩展查询条件中的每个查询项,防止传过来的参数有问题，需要做容错性处理
        :param prop_condition_item:
        :return:
        """
        if not prop_condition_item or ':' not in prop_condition_item:
            return []
        key, value_str = prop_condition_item.strip().split(':', 1)
        values = value_str.strip().split(',')

        return [{"nested": {"path": "props", "query": {
            "bool": {"must": [{"term": {"props.name": key}}, {"terms": {"props.value": values}}]}}}}]

    def parse_catpath_query_condition(self, cats_str):
        """
        解析路径查询QDSL
        :param query_condition_dic:
        :return:
        """
        category_path_list = cats_str.strip().split(',') if cats_str else []
        _query_dsl = self.get_catpath_query_qdl(len(category_path_list), category_path_list)
        return [_query_dsl] if _query_dsl else []

    def get_catpath_qdsl_last_element(self, catpath_dsl, level, max_level):
        """
        获取路径查询QDSL中的设值属性,采用递归消除for循环中的局部变量重复赋值
        :param catpath_dsl:
        :param level:
        :param max_level:
        :return:
        """
        return catpath_dsl['nested']['query']['bool'][
            'must'] if level == max_level else self.get_catpath_qdsl_last_element(catpath_dsl['nested']['query'][
                                                                                      'bool']['must'][1], level + 1,
                                                                                  max_level)

    def get_catpath_query_qdl(self, level, category_path_list):
        if level == 0:
            return {}
        elif level == 1:
            return self.get_catpath_query_item(1, category_path_list[0])

        last_item = self.get_catpath_query_qdl(level - 1, category_path_list[:(level - 1)])
        must_list = self.get_catpath_qdsl_last_element(last_item, 0, level - 2)
        must_list.append(self.get_catpath_query_item(level + 1, category_path_list[level - 1]))
        return last_item

    def get_catpath_query_item(self, level, catpath_value):
        """
        获取当前嵌套的路径查询QDSL，需要考虑路径嵌套
        :param level:
        :param catpath_value:
        :return:
        """
        path = reduce(lambda x, y: x + '.childs', repeat('cats', level - 1)) if level > 1 else 'cats'
        field = path + '.name'
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    field: self.to_array(catpath_value)
                                }
                            }
                        ]
                    }
                }
            }
        }


    @staticmethod
    def field_to_es_field(field_name):
        if field_name == 'price':
            return 'salePrice'
        return field_name

    @staticmethod
    def to_range_qdsl(value_str):
        """
        获取范围查询的QDSL， 支持100-300，-300， 100三种格式
        :param value_str:
        :return:
        """
        if not value_str:
            return ""

        ranges = value_str.split('-')
        if '-' in value_str:
            range_qdsl = {'gte': float(ranges[0].strip()) if len(ranges) == 2 else 0,
                          'lt': float(ranges[1].strip()) if len(ranges) == 2 else float(ranges[0].strip())}
        else:
            range_qdsl = {'gte': float(ranges[0].strip())}

        return range_qdsl

    def global_id_to_field(self, field_id):
        """
        全局的ID和名称映射
        :param field_id:
        :return:
        """
        return self.field_to_es_field(field_id)

    @staticmethod
    def order_num_to_es_str(order_num):
        """
        排序字段映射, 默认1代表升序，0代表降序
        :param order_num:
        :return:
        """
        order_ids = config.get_value('/consts/query/orders')
        return order_ids[order_num] if order_num in order_ids else 'asc'

    @staticmethod
    def to_array(obj):
        """
        将参数转换为list，如果已经是truple或者list，则什么也不做
        :param obj:
        :return:
        """
        return obj if isinstance(obj, tuple) or isinstance(obj, list) else [obj]

    ###########################################################################################
    # 聚合QDSL解析相关函数 start
    ###########################################################################################
    @debug_log.debug('get_agg_qdl::')
    def get_agg_qdl(self, index, type, query_params, es_connection=None):
        cats = get_dict_value(query_params, 'cats')
        catpath_agg_qdl = self.parse_catpath_agg_condition(cats)
        # cur_agg_qdl = self.agg_dsl.copy().update(catpath_agg_qdl)
        cur_agg_qdl = dict(self.get_origin_agg_qdl(), **catpath_agg_qdl)
        if 'brand' in query_params:
            # 如果品牌在查询条件中，那么就表示不需要再对品牌做聚合
            cur_agg_qdl.pop('brand', None)
        qdsl = self.get_product_query_qdsl(index, type, query_params, es_connection)
        qdsl['aggs'] = cur_agg_qdl
        qdsl['size'] = 0
        qdsl = deep_merge(qdsl, extend_parser.get_agg_qdsl(query_params))
        return qdsl


    def parse_catpath_agg_condition(self, cats_str):
        category_path_list = cats_str.strip().split(',') if cats_str else []
        cats_agg = self.get_catpath_agg_qdl(len(category_path_list))
        cats_agg['wheel_cats'] = self.get_catpath_agg_qdl(len(category_path_list) + 1)['cats']
        return cats_agg

    def get_catpath_agg_qdl(self, level):
        agg_size = config.get_value('/consts/global/agg_size') or self.DEFAULT_AGG_SIZE
        if level == 0:
            return {"cats": {
                "aggs": {
                    "name": {
                        "terms": {
                            "field": "cats.name",
                            "size": agg_size
                        }
                    }
                },
                "nested": {
                    "path": "cats"
                }
            }}

        last_item = self.get_catpath_agg_qdl(level - 1)
        dic = self.get_catpath_agg_qdsl_last_element(last_item['cats'], 0, level - 1)
        dic['aggs'] = {'childs': self.get_catpath_agg_item(level + 1)}
        return last_item

    def get_catpath_agg_qdsl_last_element(self, agg_dsl, level, max_level):
        """
        获取路径聚合QDSL中的设值属性,采用递归消除for循环中的局部变量重复赋值
        :param agg_dsl:
        :param level:
        :param max_level:
        :return:
        """
        return agg_dsl['aggs']['name'] if level == max_level else \
            self.get_catpath_agg_qdsl_last_element(agg_dsl['aggs']['name']['aggs']['childs'], level + 1, max_level)

    def get_catpath_agg_item(self, level):
        agg_size = config.get_value('/consts/global/agg_size') or self.DEFAULT_AGG_SIZE
        path = reduce(lambda x, y: x + '.childs', repeat('cats', level)) if level > 0 else 'cats'
        field = path + '.name'
        return {"aggs": {"name": {"terms": {"field": field, "size": agg_size}}}, "nested": {"path": path}}

    ###########################################################################################
    # Suggest QDSL解析相关函数 start
    ###########################################################################################
    @debug_log.debug('get_suggest_qdl')
    def get_suggest_qdl(self, index, type, query_params):
        word = get_dict_value(query_params, 'q')
        # type = get_dict_value(query_params, 'type', 1)  #暂时不支持type，type表示建议类型（可不填，默认为1），1表示搜索框的拼写建议，2表示“你要找的是不是…”
        suggest_size = get_dict_value(query_params, 'size', DEFAULT_VALUE['suggest_size']['default'],
                                      DEFAULT_VALUE['suggest_size']['min'], DEFAULT_VALUE['suggest_size']['max'])
        cur_suggest_qdl = self.suggest_qdl.copy()
        cur_suggest_qdl['completion_suggest']['text'] = word
        suggest_size_multiple = config.get_value('/consts/suggest/tag_query_multiple') or 10
        cur_suggest_qdl['completion_suggest']['completion']['size'] = int(suggest_size) * suggest_size_multiple
        return cur_suggest_qdl


    ###########################################################################################
    # Search QDSL解析相关函数 start
    ###########################################################################################
    @debug_log.debug('get_search_qdl')
    def get_search_qdl(self, index, type, query_params, es_connection=None):
        res_str = get_dict_value(query_params, 'res')
        if not res_str:
            res_str = 'products,aggregations'
        res_list = map(lambda res: res.strip(), res_str.strip().split(','))

        qdsl = self.get_product_query_qdsl(index, type, query_params, es_connection)
        if 'aggregations' in res_list:
            cats = get_dict_value(query_params, 'cats')
            catpath_agg_qdl = self.parse_catpath_agg_condition(cats)
            cur_agg_qdl = dict(self.get_origin_agg_qdl(), **catpath_agg_qdl)
            if 'brand' in query_params:
                # 如果品牌在查询条件中，那么就表示不需要再对品牌做聚合
                cur_agg_qdl.pop('brand', None)
            qdsl['aggs'] = cur_agg_qdl
        qdsl = deep_merge(qdsl, extend_parser.get_agg_qdsl(query_params))
        if 'products' not in res_list:
            qdsl['size'] = 0

        return qdsl


class ExtendQdslParser(object):
    """
    通用文档扩展查询支持
    """

    def __init__(self):
        self.regex = '(?P<op_type>[\\d\\D]+?)\\((?P<field_str>[\\d\\D]*)\\)'
        self.dsl_regex_tmpl = 'tmpl:\\((?P<tmpl>[\\d\\D]+?)\\)'
        self.dsl_regex_param = 'param:\\((?P<param>[\\d\\D]+?)\\)'
        self.variable_name_pattern = re.compile(r'{[a-zA-Z$][a-zA-Z0-9_$]*?}')
        self.section_regex_optimize = 'optimize:(?P<optimize>[\\w]+)'
        self.section_regex_size = 'size:(?P<size>[\\d]+)'
        self.highlight_query_tmpl = {"number_of_fragments": 0, "highlight_query": {"bool": {"must": []}}}
        self.QUERY_QDSL_PARSER_DICT = {'term': self.__get_query_term_fragment, 'range': self.__get_query_range_fragment,
                                       'ids': self.__get_query_ids_fragment, 'match': self.__get_query_match_fragment,
                                       'query_string': self.__get_query_querystring_fragment,
                                       'multi_match': self.__get_query_multi_match_fragment,
                                       'more_like_this': self.__get_query_more_like_this_fragment,
                                       'prefix': self.__get_query_prefix_fragment,
                                       'regexp': self.__get_query_regexp_fragment,
                                       'span_first': self.__get_query_span_first_fragment,
                                       'span_near': self.__get_query_span_near_fragment,
                                       'wildcard': self.__get_query_wildcard_fragment,
        }
        self.FILTER_QDSL_PARSER_DICT = {'geo_distance': self.__get_filter_geo_distance_fragment,
                                        'geo_distance_range': self.__get_filter_geo_distance_range_fragment,
                                        'geo_bounding_box': self.__get_filter_geo_bounding_box_fragment}
        self.AGGS_QDSL_PARSER_DICT = {'max': self.__get_agg_max_fragment,
                                      'min': self.__get_agg_min_fragment,
                                      'sum': self.__get_agg_sum_fragment,
                                      'avg': self.__get_agg_avg_fragment,
                                      'stats': self.__get_agg_stats_fragment,
                                      'exstats': self.__get_agg_exstats_fragment,
                                      'value_count': self.__get_agg_value_count_fragment,
                                      'percentiles': self.__get_agg_percentiles_fragment,
                                      'percentile_ranks': self.__get_agg_percentile_ranks_fragment,
                                      'cardinality': self.__get_agg_cardinality_fragment,
                                      'missing': self.__get_agg_missing_fragment,
                                      'terms': self.__get_agg_terms_fragment,
                                      'range': self.__get_agg_range_fragment,
                                      'date_range': self.__get_agg_date_range_fragment,
                                      'histogram': self.__get_agg_histogram_fragment,
                                      'date_histogram': self.__get_agg_date_histogram_fragment,
                                      'geo_distance': self.__get_agg_geo_distance_fragment,
                                      'cats': self.__get_agg_cats_fragment
        }

    @debug_log.debug('get_qdsl')
    def get_qdsl(self, query_params):
        """
        获取自定义查询的QDSL
        :param query_params:
        :return:
        """
        query_qdsl = self.get_query_qdsl(query_params)
        dsl_qdsl = self.get_request_dsl_qdsl(query_params)
        script_qdsl = self.get_request_py_script_qdsl(query_params)
        hight_qdsl = self.get_highlight_dsl(query_params)
        fields_qdsl = self.get_fields_dsl(query_params)
        script_fields_qdsl = self.get_script_fields_dsl(query_params)
        fielddata_fields_qdsl = self.get_fielddata_fields_dsl(query_params)
        filter_qdsl = self.get_filter_qdsl(query_params)
        return reduce(deep_merge, (
            query_qdsl, dsl_qdsl, script_qdsl, hight_qdsl, fields_qdsl, script_fields_qdsl, fielddata_fields_qdsl,
            filter_qdsl))

    def get_agg_qdsl(self, query_params):
        """
        获取自定义的聚合DSL
        :param query_params:
        :return:
        """
        # 价格自动分区aggs
        section_agg_statis_dsl = self.get_section_agg_statis_dsl(query_params)

        # 通用聚合
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_agg_')
        agg_qdsl_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_agg_qdsl_list_fragment(field_name, filed_value_list),
            ex_query_params_dict.iteritems()))
        ex_aggs_dsl = {}
        for agg_dsl in agg_qdsl_list:
            ex_aggs_dsl.update(agg_dsl)

        return deep_merge({'aggs': ex_aggs_dsl}, section_agg_statis_dsl)

    def get_section_agg_statis_dsl(self, query_params):
        """
        返回section统计字段，主要是统计平均值、方差等信息
        ex_section_salePrice=section(optimize:true,size:6)
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_section_')
        sum_agg_static_dsl = {}
        for (key, value_list) in ex_query_params_dict.iteritems():
            variable_name, field_name = unbind_variable('ex_section_(?P<field_name>[\\d\\D]+)', 'field_name', key)
            agg_static_dsl = {'aggs': {field_name + '_stats': {"extended_stats": {"field": field_name}}}}
            sum_agg_static_dsl = deep_merge(sum_agg_static_dsl, agg_static_dsl)
        return sum_agg_static_dsl

    def get_section_agg_range_dsl(self, query_params, agg_es_result):
        """
        返回section range aggreations dsl
        :param query_params:
        :param avg:
        :param std_deviation:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_section_')
        sum_agg_range_dsl = {}
        if not agg_es_result:
            return sum_agg_range_dsl
        for (key, value_list) in ex_query_params_dict.iteritems():
            value = value_list[0]
            var_name, field_name = unbind_variable('ex_section_(?P<field_name>[\\d\\D]+)', 'field_name', key)
            agg_stats_result_key = field_name + '_stats'
            if agg_stats_result_key not in agg_es_result:
                continue
            avg = agg_es_result[agg_stats_result_key]['avg']
            std_deviation = agg_es_result[agg_stats_result_key]['std_deviation']
            if avg is None or std_deviation is None:
                continue
            section_num = int(unbind_variable(self.section_regex_size, 'size', value)[1] or '0')
            agg_range_list = equal_section_partitions.get_child_section_range_list(avg, std_deviation, section_num)
            agg_range_dsl = {
                'aggs': {field_name + '_range': {"range": {"ranges": agg_range_list, "field": field_name}}}}
            sum_agg_range_dsl = deep_merge(sum_agg_range_dsl, agg_range_dsl)
        return sum_agg_range_dsl

    def get_fields_dsl(self, query_params):
        """
        获取fields查询dsl，格式为：ex_fields=spuId,salePrice,skuId
        :param query_params:
        :return:
        """
        fields_str = query_params.get('ex_fields')
        if not fields_str:
            return {}
        field_name_list = fields_str.split(',')
        if field_name_list:
            return {'fields': field_name_list}
        return {}

    def get_script_fields_dsl(self, query_params):
        """
        Script field 查询  格式为：ex_script_field_属性名=doc[salePrice].value*2
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_script_field_')
        script_fields_sub_dsl = {}
        for key_str in ex_query_params_dict:
            var_name, field_name = unbind_variable('ex_script_field_(?P<field_name>[\\d\\D]+)', 'field_name', key_str)
            script_str = self.format_script_str(ex_query_params_dict[key_str][0])
            if script_str:
                script_fields_sub_dsl[field_name] = {'script': script_str}
        return {'script_fields': script_fields_sub_dsl} if script_fields_sub_dsl else {}

    def get_fielddata_fields_dsl(self, query_params):
        """
        fielddata_fields 缓存（不对外提供）
        :param query_params:
        :return:
        """
        fields_str = query_params.get('ex_fielddatas')
        if not fields_str:
            return {}
        field_name_list = fields_str.split(',')
        if field_name_list:
            return {'fielddata_fields': field_name_list}
        return {}

    def get_highlight_dsl(self, query_params):
        """
        获取高亮查询DSL, url高亮的查询语法为：ex_highlight_title=highlight(q:kkkkyyy,pre_tags:<em>,post_tags:</em>,multi_field:title.standard)
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_highlight_')
        sum_highlight_dsl = {}
        global_query_string = get_dict_value(query_params, 'q')
        global_query_string_dsl = None
        es_connection = EsConnectionFactory.get_es_connection(host=settings.SERVICE_BASE_CONFIG['elasticsearch'])
        for (key, value_list) in ex_query_params_dict.iteritems():
            var_name, field_name = unbind_variable('ex_highlight_(?P<field_name>[\\d\\D]+)', 'field_name', key)
            temp, value = unbind_variable('\\((?P<value>[\\d\\D]+)\\)', 'value', value_list[0])
            pre_tags = unbind_variable('pre_tags:(?P<pre_tags>[\\d\\D]+?),', 'pre_tags', value)[1] or \
                       unbind_variable('pre_tags:(?P<pre_tags>[\\d\\D]+)', 'pre_tags', value)[1] or '<hl>'
            post_tags = unbind_variable('post_tags:(?P<post_tags>[\\d\\D]+?),', 'post_tags', value)[1] or \
                        unbind_variable('post_tags:(?P<post_tags>[\\d\\D]+)', 'post_tags', value)[1] or '</hl>'
            query_string = unbind_variable('q:(?P<q>[\\d\\D]+?),', 'q', value)[1] or \
                           unbind_variable('q:(?P<q>[\\d\\D]+)', 'q', value)[1] or global_query_string
            multi_field_name = unbind_variable('multi_field:(?P<multi_field>[\\d\\D]+?),', 'multi_field', value)[1] or \
                               unbind_variable('multi_field:(?P<multi_field>[\\d\\D]+)', 'multi_field', value)[1] \
                               or field_name
            if not query_string:
                continue
            if query_string == global_query_string:
                global_query_string_dsl = global_query_string_dsl or qdsl_parser.parse_query_string_condition(
                    query_string, es_connection, multi_field_name)
                basic_query_string_dsl = global_query_string_dsl
            else:
                basic_query_string_dsl = qdsl_parser.parse_query_string_condition(query_string, es_connection,
                                                                                  multi_field_name)

            highlight_query_tmpl = dict(self.highlight_query_tmpl)
            highlight_query_tmpl['highlight_query']['bool']['must'] = basic_query_string_dsl
            highlight_query_tmpl['pre_tags'] = [pre_tags]
            highlight_query_tmpl['post_tags'] = [post_tags]
            highlight_dsl = {"highlight": {"fields": {multi_field_name: highlight_query_tmpl}}}
            sum_highlight_dsl = deep_merge(sum_highlight_dsl, highlight_dsl)
        return sum_highlight_dsl

    def get_highlight_field_to_origin(self, query_params, highlight_field_name):
        """
        将高亮字段转化为原始字段，主要是针对multi-field的情况
        :param query_params:
        :param highlight_field_name:
        :return:
        """
        if 'ex_highlight_' + highlight_field_name in query_params:
            return highlight_field_name
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_highlight_')
        for (key, value_list) in ex_query_params_dict.iteritems():
            value = value_list[0]
            if value.index('multi_field:' + highlight_field_name) > -1:
                var_name, field_name = unbind_variable('ex_highlight_(?P<field_name>[\\d\\D]+)', 'field_name', key)
                return field_name
        query_log.error('Fail to get highlight field to origin : {0}', highlight_field_name)


    @debug_log.debug('get_query_qdsl')
    def get_query_qdsl(self, query_params):
        """
        获取所有额外查询的qdsl
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_q_')
        qdsl_must_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_query_qdsl_list_fragment(field_name, filed_value_list),
            ex_query_params_dict.iteritems()))
        return {'query': {'bool': {'must': qdsl_must_list}}} if qdsl_must_list else {}

    @debug_log.debug('get_filter_qdsl')
    def get_filter_qdsl(self, query_params):
        """
        获取所有filter查询的dsl
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_f_')
        qdsl_must_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_filter_qdsl_list_fragment(field_name, filed_value_list),
            ex_query_params_dict.iteritems()))
        return {'query': {'filter': {'bool': {'must': qdsl_must_list}}}} if qdsl_must_list else {}

    def get_request_dsl_qdsl(self, query_params):
        """
        处理客户端请求中的DSL，输入格式为: ex_dsl :  tmpl={json},param={json}
        :param query_params:
        :return:
        """
        input_qdsl_param_str = query_params.get('ex_dsl')
        if not input_qdsl_param_str:
            return {}
        temp_name, dsl_tmpl = unbind_variable(self.dsl_regex_tmpl, 'tmpl', input_qdsl_param_str)
        temp_name, param_str = unbind_variable(self.dsl_regex_param, 'param', input_qdsl_param_str)
        if not dsl_tmpl:
            return None
        if param_str:
            param_dict = json.loads(param_str)
            bind_variable = lambda match_obj: match_obj.group(0).format(**param_dict)
            dsl_tmpl = self.variable_name_pattern.sub(bind_variable, dsl_tmpl)
        return json.loads(dsl_tmpl)

    def get_request_py_script_qdsl(self, query_params):
        """
        处理客户端请求中的python脚本,输入格式为：  ex_script_py   :  script_data_parsers.parse_skuids_from_pc_mq_msg
        脚本的输入参数为request
        :param query_params:
        :return:
        """
        input_py_path_str = query_params.get('ex_script_py')
        if not input_py_path_str:
            return {}
        function = python_invoker.reflect_obj(input_py_path_str)
        if function:
            return function(query_params)


    def get_query_qdsl_list_fragment(self, field_name, filed_value_list):
        """
        获取符合查询条件QDSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_q_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_query_qdsl_single_fragment(field_name, input_str),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return {'bool': {'should': qdsl_fragment_list, 'minimum_should_match': 1}}

    def get_filter_qdsl_list_fragment(self, field_name, filed_value_list):
        """
        获取符合过滤条件QDSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_f_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_filter_qdsl_single_fragment(field_name, input_str),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return {'bool': {'should': qdsl_fragment_list, 'minimum_should_match': 1}}

    def get_agg_qdsl_list_fragment(self, field_name, filed_value_list):
        """
        获取符合聚合DSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_agg_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_agg_qdsl_single_fragment(field_name, input_str),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return reduce(deep_merge, qdsl_fragment_list)


    def get_query_qdsl_single_fragment(self, field_name, input_str):
        """
        获取单个查询条件QDSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex, 'field_str', input_str)
        if field_str is None:
            query_log.info('Get query qdsl fragment the filed_str is null')
            return None
        if op_type not in self.QUERY_QDSL_PARSER_DICT:
            query_log.warning('Get query qdsl fragment has not support op type {0}', op_type)
            return None
        return self.QUERY_QDSL_PARSER_DICT[op_type](field_name, field_str)

    def get_filter_qdsl_single_fragment(self, field_name, input_str):
        """
        获取单个过滤条件QDSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex, 'field_str', input_str)
        if field_str is None:
            query_log.info('Get filter qdsl fragment the filed_str is null')
            return None
        if op_type not in self.QUERY_QDSL_PARSER_DICT:
            query_log.warning('Get filter qdsl fragment has not support op type {0}', op_type)
            return None
        return self.FILTER_QDSL_PARSER_DICT[op_type](field_name, field_str)

    def get_agg_qdsl_single_fragment(self, field_name, input_str):
        """
        获取单个聚合DSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex, 'field_str', input_str)
        field_str = field_str or ''
        if op_type not in self.AGGS_QDSL_PARSER_DICT:
            query_log.warning('Get agg qdsl fragment has not support op type {0}', op_type)
            return None
        return self.AGGS_QDSL_PARSER_DICT[op_type](field_name, field_str)


    def get_query_params_by_prefix(self, query_params, prefix_str):
        """
        根据前缀过滤http request请求的参数名称
        :param query_params:
        :param prefix_str:
        :return:
        """
        return dict(((query_param_name, query_params.getlist(query_param_name)) for query_param_name in
                     query_params.iterkeys() if
                     query_param_name.startswith(prefix_str)))


    def __get_query_term_fragment(self, filed_name, field_str):
        """
        term查询解析
        :param filed_name:
        :param field_str:
        :return:
        """
        term_values = self.__parse_single_input_str(field_str)
        return {
            "terms": {
                filed_name: list(term_values),
                "minimum_should_match": 1
            }
        }

    def __get_query_ids_fragment(self, filed_name, field_str):
        """
        ids查询
        :param filed_name:
        :param field_str:
        :return:
        """
        id_values = self.__parse_single_input_str(field_str)
        return {
            "ids": {
                "values": list(id_values),
            }
        }

    def __get_query_range_fragment(self, filed_name, field_str):
        """
        范围查询
        :param filed_name:
        :param field_str:
        :return:
        """

        def __range_value_to_qdsl(floor_value, ceiling_vlaue):
            range_qsdl_item = {}
            if floor_value:
                range_qsdl_item['gte'] = floor_value
            if ceiling_vlaue:
                range_qsdl_item['lt'] = ceiling_vlaue
            return range_qsdl_item

        range_values = self.__parse_range_input_str(field_str)
        range_qdsl_list = map(lambda (floor_value, ceiling_vlaue): {
            "range": {filed_name: __range_value_to_qdsl(floor_value, ceiling_vlaue)}}, range_values)

        return {'bool': {'should': range_qdsl_list, 'minimum_should_match': 1}}

    def __get_query_querystring_fragment(self, field_name, field_str):
        """
        query_string 查询
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None
        fields = field_name.split(',')
        search_item_str_list = field_str.split(';')
        query_string_dsl = {}
        if fields:
            query_string_dsl = {"fields": fields}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'query':
                    query_string_dsl[search_item_key_value[0]] = search_item_str[6:]
                elif search_item_key_value[0] in (
                        'fuzzy_max_expansions', 'fuzzy_prefix_length', 'phrase_slop', 'boost',
                        'max_determinized_states',
                        'minimum_should_match'):
                    query_string_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] in (
                        'allow_leading_wildcard', 'lowercase_expanded_terms', 'enable_position_increments',
                        'analyze_wildcard', 'lenient'):
                    query_string_dsl[search_item_key_value[0]] = bool(search_item_key_value[1])
                elif search_item_key_value[0] == 'fields':
                    query_string_dsl[search_item_key_value[0]] = search_item_key_value[1].split(',')
                else:
                    query_string_dsl[search_item_key_value[0]] = search_item_key_value[1]
        if 'query' not in query_string_dsl:
            # 没有query_string关键词，抛出异常
            raise InvalidParamError(
                'Query_string search don\'t has query string, {0} , {1}'.format(field_name, field_str))
        return {"query_string": query_string_dsl}

    def __get_query_match_fragment(self, field_name, field_str):
        """
        match 查询
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None
        return {
            "match": {
                field_name: field_str
            }
        }

    def __get_query_multi_match_fragment(self, field_name, field_str):
        """
        multi_match match 查询
        ex_q_title,salePrice,cats = multi_match(query:abcdefg;fields:title,salePrice;type:most_fields;operator:and)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None
        fields = field_name.split(',')
        search_item_str_list = field_str.split(';')
        multi_match_dsl = {
            "fields": fields,
        }
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'tie_breaker':
                    multi_match_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'fields':
                    multi_match_dsl[search_item_key_value[0]] = search_item_key_value[1].split(',')
                else:
                    multi_match_dsl[search_item_key_value[0]] = search_item_key_value[1]
        if 'query' not in multi_match_dsl:
            # 没有match关键词，抛出异常
            raise InvalidParamError(
                'Multi-match search don\'t has query string, {0} , {1}'.format(field_name, field_str))
        return {"multi_match": multi_match_dsl}

    def __get_query_more_like_this_fragment(self, field_name, field_str):
        """
        more_like_this 查询
        格式为：ex_q_title,salePrice,cats=more_like_this(like_text:华为手机)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None
        fields = field_name.split(',')
        search_item_str_list = field_str.split(';')
        more_like_this_dsl = {
            "fields": fields,
        }
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] in (
                        'min_term_freq', 'max_query_terms', 'min_doc_freq', 'max_doc_freq', 'min_word_length',
                        'max_word_length', 'minimum_should_match', 'boost'):
                    more_like_this_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] in ('ids', 'stop_words'):
                    more_like_this_dsl[search_item_key_value[0]] = search_item_key_value[1].split(',')
                elif search_item_key_value[0] == 'docs':
                    more_like_this_dsl[search_item_key_value[0]] = map(lambda doc_str: json.loads(doc_str.strip()),
                                                                       search_item_key_value[1].split(','))
                else:
                    more_like_this_dsl[search_item_key_value[0]] = search_item_key_value[1]

        return {"more_like_this": more_like_this_dsl}

    def __get_query_prefix_fragment(self, field_name, field_str):
        """
        根据前缀查询
        ex_q_title=prefix(prefix:华为手机;boost:1.0)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        prefix_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'boost':
                    prefix_dsl[field_name][search_item_key_value[0]] = float(search_item_key_value[1])
                else:
                    prefix_dsl[field_name][search_item_key_value[0]] = search_item_key_value[1]
        if 'value' not in prefix_dsl[field_name] and 'prefix' not in prefix_dsl[field_name]:
            # 没有prefix关键词，抛出异常
            raise InvalidParamError(
                'Prefix search don\'t has query string, {0} , {1}'.format(field_name, field_str))
        return {"prefix": prefix_dsl}

    def __get_query_regexp_fragment(self, field_name, field_str):
        """
        根据正则表达式查询
        Ex_q__属性名=regexp(value:华为*手机;flags:ALL;max_determinized_states:20000;boost:2.0)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        regexp_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] in ('boost', 'max_determinized_states'):
                    regexp_dsl[field_name][search_item_key_value[0]] = float(search_item_key_value[1])
                else:
                    regexp_dsl[field_name][search_item_key_value[0]] = search_item_key_value[1]
        if 'value' not in regexp_dsl[field_name]:
            # 没有prefix关键词，抛出异常
            raise InvalidParamError(
                'Regexp search don\'t has value string, {0} , {1}'.format(field_name, field_str))
        return {"regexp": regexp_dsl}

    def __get_query_span_first_fragment(self, field_name, field_str):
        """
        Span_first
        Span_first 只允许返回在字段前几个位置上匹配查询条件的文档
        ex_q_属性名=span_first(value:华为;end:3)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        span_first_dsl = {}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'value':
                    span_first_dsl['match'] = {'span_term': {field_name: search_item_key_value[1]}}
                elif search_item_key_value[0] in ('boost', 'end'):
                    span_first_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                else:
                    span_first_dsl[search_item_key_value[0]] = search_item_key_value[1]
        if 'match' not in span_first_dsl:
            # 没有prefix关键词，抛出异常
            raise InvalidParamError(
                'Span first search don\'t has value string, {0} , {1}'.format(field_name, field_str))
        return {"span_first": span_first_dsl}

    def __get_query_span_near_fragment(self, field_name, field_str):
        """
        span_near 查询
        格式为：ex_q_title=span_near(value:华为,手机;slop:10;in_order:true;collect_payloads:false)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None
        search_item_str_list = field_str.split(';')
        span_near_dsl = {}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'value':
                    value_list = search_item_key_value[1].split(',')
                    span_near_dsl['clauses'] = map(lambda term_value: {'span_term': {field_name: term_value}},
                                                   value_list)
                elif search_item_key_value[0] in ('slop', 'boost'):
                    span_near_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] in ('in_order', 'collect_payloads'):
                    span_near_dsl[search_item_key_value[0]] = bool(search_item_key_value[1])
                else:
                    span_near_dsl[search_item_key_value[0]] = search_item_key_value[1]
        if 'clauses' not in span_near_dsl:
            # 没有near关键词，抛出异常
            raise InvalidParamError(
                'Span near search don\'t has value string, {0} , {1}'.format(field_name, field_str))

        return {"span_near": span_near_dsl}

    def __get_query_wildcard_fragment(self, field_name, field_str):
        """
        根据通配符查询
        和term查询类似，不过可以使用通配符
        ex_q_属性名=wildcard(value:华*为;boost:2)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        wildcard_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'boost':
                    wildcard_dsl[field_name][search_item_key_value[0]] = float(search_item_key_value[1])
                else:
                    wildcard_dsl[field_name][search_item_key_value[0]] = search_item_key_value[1]
        if 'value' not in wildcard_dsl[field_name]:
            # 没有prefix关键词，抛出异常
            raise InvalidParamError(
                'Wildcard search don\'t has value string, {0} , {1}'.format(field_name, field_str))
        return {"wildcard": wildcard_dsl}

    def __get_filter_geo_distance_fragment(self, field_name, field_str):
        """
        距离查询
        Geo Distance Filter
        Ex_f_distance=geo_distance(location:12.0,2.35;distance:100km)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        geo_distance_filter_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'boost':
                    geo_distance_filter_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'location':
                    geo_distance_filter_dsl[field_name] = float(search_item_key_value[1])
                else:
                    geo_distance_filter_dsl[search_item_key_value[0]] = search_item_key_value[1]
        return {"geo_distance": geo_distance_filter_dsl}

    def __get_filter_geo_distance_range_fragment(self, field_name, field_str):
        """
        距离范围查询
        Geo Distance Range Filter
        Ex_f_distance=geo_distance_range(location:12.0,2.35;from:100km;to:500km)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        geo_distance_filter_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'boost':
                    geo_distance_filter_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'location':
                    geo_distance_filter_dsl[field_name] = float(search_item_key_value[1])
                else:
                    geo_distance_filter_dsl[search_item_key_value[0]] = search_item_key_value[1]
        return {"geo_distance_range": geo_distance_filter_dsl}

    def __get_filter_geo_bounding_box_fragment(self, field_name, field_str):
        """
        Geo Bounding box Filter
        地理位置矩形查询
        Ex_f_distance=geo_bounding_box(top_left:12.0,2.35;bottom_right:55,66)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str or not field_name:
            return None
        search_item_str_list = field_str.split(';')
        geo_distance_filter_dsl = {field_name: {}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'boost':
                    geo_distance_filter_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                else:
                    geo_distance_filter_dsl[search_item_key_value[0]] = search_item_key_value[1]
        return {"geo_bounding_box": geo_distance_filter_dsl}

    def __get_agg_max_fragment(self, field_name, field_str):
        """
        agg max聚合
        ex_agg_title=max(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.max'
        agg_max_dsl = {"max": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_max_dsl['max']['script'] = search_item_key_value[1]
                    del agg_max_dsl['max']['field']
        return {aggs_key: agg_max_dsl}

    def __get_agg_min_fragment(self, field_name, field_str):
        """
        agg min聚合
        ex_agg_title=min(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.min'
        search_item_str_list = field_str.split(';')
        agg_min_dsl = {"min": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_min_dsl['min']['script'] = search_item_key_value[1]
                    del agg_min_dsl['min']['field']
        return {aggs_key: agg_min_dsl}

    def __get_agg_sum_fragment(self, field_name, field_str):
        """
        agg sum聚合
        ex_agg_title=sum(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.sum'
        search_item_str_list = field_str.split(';')
        agg_sum_dsl = {"sum": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_sum_dsl['sum']['script'] = search_item_key_value[1]
                    del agg_sum_dsl['sum']['field']
        return {aggs_key: agg_sum_dsl}

    def __get_agg_avg_fragment(self, field_name, field_str):
        """
        agg avg
        ex_agg_title=avg(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.avg'
        search_item_str_list = field_str.split(';')
        agg_avg_dsl = {"avg": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_avg_dsl['avg']['script'] = search_item_key_value[1]
                    del agg_avg_dsl['avg']['field']
        return {aggs_key: agg_avg_dsl}

    def __get_agg_stats_fragment(self, field_name, field_str):
        """
        agg stats
        ex_agg_title=stats(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.stats'
        search_item_str_list = field_str.split(';')
        agg_stats_dsl = {"stats": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_stats_dsl['stats']['script'] = search_item_key_value[1]
                    del agg_stats_dsl['stats']['field']
        return {aggs_key: agg_stats_dsl}

    def __get_agg_exstats_fragment(self, field_name, field_str):
        """
        agg extended_stats
        ex_agg_title=exstats(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.exstats'
        search_item_str_list = field_str.split(';')
        agg_exstats_dsl = {"extended_stats": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_exstats_dsl['extended_stats']['script'] = search_item_key_value[1]
                    del agg_exstats_dsl['extended_stats']['field']
        return {aggs_key: agg_exstats_dsl}

    def __get_agg_value_count_fragment(self, field_name, field_str):
        """
        agg value count
        ex_agg_title=value_count(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.value_count'
        search_item_str_list = field_str.split(';')
        agg_value_count_dsl = {"value_count": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_value_count_dsl['value_count']['script'] = search_item_key_value[1]
                    del agg_value_count_dsl['value_count']['field']
        return {aggs_key: agg_value_count_dsl}

    def __get_agg_percentiles_fragment(self, field_name, field_str):
        """
        agg value count
        ex_agg_title=percentiles(script:doc.price.value)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.percentiles'
        search_item_str_list = field_str.split(';')
        agg_percentiles_dsl = {"percentiles": {"field": field_name}}
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_percentiles_dsl['percentiles']['script'] = search_item_key_value[1]
                    del agg_percentiles_dsl['percentiles']['field']
                elif search_item_key_value[0] == 'percents':
                    agg_percentiles_dsl['percentiles']['percents'] = map(lambda str_percent: float(str_percent),
                                                                         search_item_key_value[1].split(','))
        return {aggs_key: agg_percentiles_dsl}

    def __get_agg_percentile_ranks_fragment(self, field_name, field_str):
        """
        Percentile Ranks Aggregation
        根据数值查询百分比
        Ex_agg_salePrice=percentile_ranks(script:doc.price.value,values:10,20,30,40,100)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.percentile_ranks'
        agg_percentiles_dsl = {"percentile_ranks": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_percentiles_dsl['percentile_ranks']['script'] = search_item_key_value[1]
                    del agg_percentiles_dsl['percentile_ranks']['field']
                elif search_item_key_value[0] == 'values':
                    agg_percentiles_dsl['percentile_ranks']['values'] = map(lambda str_percent: float(str_percent),
                                                                            search_item_key_value[1].split(','))
        return {aggs_key: agg_percentiles_dsl}

    def __get_agg_cardinality_fragment(self, field_name, field_str):
        """
        Cardinality Aggregation
        查询不同值的数码，类似于数据库的distinct
        Ex_agg_spuId=cardinality(precision_threshold:100)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.cardinality'
        agg_cardinality_dsl = {"cardinality": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'precision_threshold':
                    agg_cardinality_dsl['cardinality']['precision_threshold'] = float(search_item_key_value[1])
        return {aggs_key: agg_cardinality_dsl}

    def __get_agg_missing_fragment(self, field_name, field_str):
        """
        Missing Aggregation
        统计字段为null或者没有该字段的doc数目
        Ex_agg_salePrice=missing()
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.missing'
        agg_missing_dsl = {"missing": {"field": field_name}}
        return {aggs_key: agg_missing_dsl}

    def __get_agg_terms_fragment(self, field_name, field_str):
        """
        Terms Aggregation
        对字段进行词条统计
        Ex_agg_salePrice=terms(size:10;order:(_term:asc);include:*pattern;
        exclude:*hhh;script:doc.price.value*2;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.terms'
        agg_terms_dsl = {"terms": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_terms_dsl['terms']['script'] = search_item_key_value[1]
                    del agg_terms_dsl['terms']['field']
                elif search_item_key_value[0] == 'min_doc_count':
                    agg_terms_dsl['terms']['min_doc_count'] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'order':
                    order_item_list = search_item_key_value[1][1:-1].split(':')
                    if len(order_item_list) == 2:
                        agg_terms_dsl['terms']['order'] = {order_item_list[0]: order_item_list[1]}
                else:
                    agg_terms_dsl['terms'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_terms_dsl}

    def __get_agg_range_fragment(self, field_name, field_str):
        """
        Range Aggregation
        范围统计
        Ex_agg_salePrice=range(ranges:num:45-num:666,num:99-,-num:200;keyed:true;
        script:doc.price.value*2;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """

        def get_ranges_dict(floor_value, ceiling_vlaue):
            """
            获取from to的dict
            """
            result = {}
            if floor_value is not None:
                result['from'] = floor_value
            if ceiling_vlaue is not None:
                result['to'] = ceiling_vlaue
            return result

        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.range'
        agg_range_dsl = {"range": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_range_dsl['range']['script'] = search_item_key_value[1]
                    del agg_range_dsl['range']['field']
                elif search_item_key_value[0] == 'min_doc_count':
                    agg_range_dsl['range']['min_doc_count'] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'keyed':
                    agg_range_dsl['range']['keyed'] = bool(search_item_key_value[1])
                elif search_item_key_value[0] == 'ranges':
                    range_list = self.__parse_range_input_str(search_item_key_value[1])
                    range_item_list = map(
                        lambda (floor_value, ceiling_vlaue): get_ranges_dict(floor_value, ceiling_vlaue), range_list)
                    if range_item_list:
                        agg_range_dsl['range']['ranges'] = range_item_list
                else:
                    agg_range_dsl['range'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_range_dsl}

    def __get_agg_date_range_fragment(self, field_name, field_str):
        """
        14. Date Range Aggregation
        时间范围统计
        Ex_agg_addDate=date_range(ranges:str:now-10M/M--str:now+10M/M,now-10M/M--,--now-10M/M;keyed:true;script:doc.price.value*2;min_doc_count:10)
        范围 from-to，原本使用-分割，但是日期中存在-，容易辉耀，因此范围使用双--
        :param field_name:
        :param field_str:
        :return:
        """

        def get_ranges_dict(floor_value, ceiling_vlaue):
            """
            获取from to的dict
            """
            result = {}
            if floor_value is not None:
                result['from'] = floor_value
            if ceiling_vlaue is not None:
                result['to'] = ceiling_vlaue
            return result

        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.date_range'
        agg_date_range_dsl = {"date_range": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'script':
                    agg_date_range_dsl['date_range']['script'] = search_item_key_value[1]
                    del agg_date_range_dsl['date_range']['field']
                elif search_item_key_value[0] == 'min_doc_count':
                    agg_date_range_dsl['date_range']['min_doc_count'] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'keyed':
                    agg_date_range_dsl['date_range']['keyed'] = bool(search_item_key_value[1])
                elif search_item_key_value[0] == 'ranges':
                    range_list = self.__parse_range_input_str(search_item_key_value[1], '--')
                    range_item_list = map(
                        lambda (floor_value, ceiling_vlaue): get_ranges_dict(floor_value, ceiling_vlaue), range_list)
                    if range_item_list:
                        agg_date_range_dsl['date_range']['ranges'] = range_item_list
                else:
                    agg_date_range_dsl['date_range'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_date_range_dsl}

    def __get_agg_histogram_fragment(self, field_name, field_str):
        """
        Histogram Aggregation
        直方图统计，
        ex_agg_salePrice=histogram(interval:100;keyed:true;order:(_term:asc);
        extended_bounds:num:45-num:666;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """

        def get_ranges_dict(floor_value, ceiling_vlaue):
            """
            获取max min的dict
            """
            result = {}
            if floor_value is not None:
                result['min'] = floor_value
            if ceiling_vlaue is not None:
                result['max'] = ceiling_vlaue
            return result

        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.histogram'
        agg_histogram_dsl = {"histogram": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] in ('min_doc_count', 'interval'):
                    agg_histogram_dsl['histogram'][search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'keyed':
                    agg_histogram_dsl['histogram']['keyed'] = bool(search_item_key_value[1])
                elif search_item_key_value[0] == 'extended_bounds':
                    range_list = self.__parse_range_input_str(search_item_key_value[1])
                    range_item_list = map(
                        lambda (floor_value, ceiling_vlaue): get_ranges_dict(floor_value, ceiling_vlaue), range_list)
                    if range_item_list:
                        agg_histogram_dsl['histogram']['extended_bounds'] = range_item_list[0]
                elif search_item_key_value[0] == 'order':
                    order_item_list = search_item_key_value[1][1:-1].split(':')
                    if len(order_item_list) == 2:
                        agg_histogram_dsl['histogram']['order'] = {order_item_list[0]: order_item_list[1]}
                else:
                    agg_histogram_dsl['histogram'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_histogram_dsl}

    def __get_agg_date_histogram_fragment(self, field_name, field_str):
        """
        Date Histogram Aggregation
        时间直方图统计
        ex_agg_updateDate=date_histogram(interval:1.5h;keyed:true;format:yyyy-MM-dd;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """

        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.date_histogram'
        agg_date_histogram_dsl = {"date_histogram": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'min_doc_count':
                    agg_date_histogram_dsl['date_histogram'][search_item_key_value[0]] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'order':
                    order_item_list = search_item_key_value[1][1:-2].split(':')
                    if len(order_item_list) == 2:
                        agg_date_histogram_dsl['date_histogram']['order'] = {order_item_list[0]: order_item_list[1]}
                elif search_item_key_value[0] == 'keyed':
                    agg_date_histogram_dsl['histogram']['keyed'] = bool(search_item_key_value[1])
                else:
                    agg_date_histogram_dsl['date_histogram'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_date_histogram_dsl}

    def __get_agg_geo_distance_fragment(self, field_name, field_str):
        """
        Geo Distance Aggregation
        地理距离统计
        Ex_agg_location=geo_distance(ranges:num:45-num:666,num:99-,-num:200;origin:52.3760, 4.894;unit:km;
        distance_type:sloppy_arc;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """

        def get_ranges_dict(floor_value, ceiling_vlaue):
            """
            获取from to的dict
            """
            result = {}
            if floor_value is not None:
                result['from'] = floor_value
            if ceiling_vlaue is not None:
                result['to'] = ceiling_vlaue
            return result

        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.geo_distance'
        agg_date_range_dsl = {"geo_distance": {"field": field_name}}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'min_doc_count':
                    agg_date_range_dsl['geo_distance']['min_doc_count'] = float(search_item_key_value[1])
                elif search_item_key_value[0] == 'ranges':
                    range_list = self.__parse_range_input_str(search_item_key_value[1])
                    range_item_list = map(
                        lambda (floor_value, ceiling_vlaue): get_ranges_dict(floor_value, ceiling_vlaue), range_list)
                    if range_item_list:
                        agg_date_range_dsl['geo_distance']['ranges'] = range_item_list
                else:
                    agg_date_range_dsl['geo_distance'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_date_range_dsl}

    def __get_agg_cats_fragment(self, field_name, field_str):
        """
        聚合商品类目路径，一次聚合出所有层级的类目
        ex_agg_cats=cats(depth:3)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.cats'
        search_item_str_list = field_str.split(';')
        depth = config.get_value('/consts/query/agg_cats_default_depth')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'depth':
                    depth = int(search_item_key_value[1])
                    break
        return {aggs_key: qdsl_parser.get_catpath_agg_qdl(depth)['cats']}

    def __parse_single_input_str(self, single_query_str):
        """
        解析单个输入字符串
        :param single_query_str:
        :return:
        """
        term_value_strs = single_query_str.split(',')
        return map(self.__get_obj_by_desc, term_value_strs)

    def __parse_range_input_str(self, range_query_str, split_char='-'):
        """
        解析范围输入字符串
        :param range_query_str:
        :return:
        """

        def __parse_single_range_str(range_single_input_str):
            """
            解析单个范围字符串，'num:45-num:5555'  'num:45-' '-num:699' '700-'
            """
            if '-' not in range_single_input_str:
                return None, None
            temps = range_single_input_str.split(split_char)
            return self.__get_obj_by_desc(temps[0]), self.__get_obj_by_desc(temps[1])

        term_value_strs = range_query_str.split(',')
        return filter(lambda (floor_value, ceiling_vlaue): floor_value or ceiling_vlaue,
                      map(__parse_single_range_str, term_value_strs))

    def __get_obj_by_desc(self, desc_str):
        """
        根据对象描述字符串生成对象，格式为: 'num:122.5', 'bool:false', 'str:kkwq'
        :param desc_str:
        :return:
        """
        if not desc_str:
            return None

        if ':' in desc_str:
            temp_strs = desc_str.split(':')
            type_str = temp_strs[0]
            value_str = temp_strs[1]
        else:
            value_str = desc_str
            type_str = 'str'

        if type_str == 'bool':
            return value_str == 'True' or value_str == 'true'
        elif type_str == 'num':
            return float(value_str)
        else:
            return value_str

    def format_script_str(self, script_str):
        """
        格式化ES script脚本
        :param script_str:
        :return:
        """
        return script_str.replace('[', '[\'').replace(']', '\']')


qdsl_parser = QdslParser()
extend_parser = ExtendQdslParser()
