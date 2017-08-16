# -*- coding: utf-8 -*-
from itertools import *

from common.adapter import es_adapter
from common.configs import config
from common.loggers import debug_log
from common.utils import get_dict_value, unbind_variable, deep_merge
from service.ex_dsl_parser import extend_parser

__author__ = 'liuzhaoming'

DEFAULT_VALUE = config.get_value('/consts/global/query_size')


class QdslParser(object):
    """
    搜索平台DSL解析
    """
    DEFAULT_AGG_SIZE = 1000

    suggest_qdl = {
        "completion_suggest": {
            "text": "",
            "completion": {
                "field": "suggest",
                "size": 10,
                "context": {
                    "type": ""
                }
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

    def get_product_query_qdsl(self, es_config, index, type, query_params, parse_fields, es_connection=None):
        """
        获取商品查询DSL
        :param es_config
        :param index:
        :param type:
        :param query_params:
        :param parse_fields
        :param es_connection
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
                  self.parse_catpath_query_condition(cats),
                  self.parse_add_field_query_qdsl(es_config, parse_fields)))

        product_query_qdsl = reduce(lambda dict_1, dict_2: dict(dict_1, **dict_2),
                                    filter(lambda item: item,
                                           [{'query': {'bool': {'must': must_bool_query_qdsl_list}}},
                                            self.parse_page_param(from_num, size_num), self.parse_sort_params(sort)]))
        extend_query_qdsl = extend_parser.get_qdsl(query_params)
        return deep_merge(product_query_qdsl, extend_query_qdsl)

    def parse_add_field_query_qdsl(self, es_config, query_params):
        """
        获取附加字段查询DSL，目前主要是用户的AdminId
        :param es_config:
        :param query_params:
        :return:
        """
        if not query_params or not query_params.get('adminId') or not es_config.get('add_admin_id_field'):
            return []
        admin_id = query_params.get('adminId')
        return [{'term': {'_adminId': admin_id}}]

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

        _analyzer = config.get_value('/consts/query/query_string/match_selected_fields/analyzer') or 'standard'
        default_index_name = config.get_value('/consts/query/default_index')
        fields_cfg = config.get_value('/consts/query/query_string/match_selected_fields/fields') or {}
        if query_string:
            analyze_token_list = es_adapter.query_text_analyze_result_without_filter(es_connection, _analyzer,
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

    def _get_nest_dsl(self, path, level, cur_path, item_dsl):
        """
        获取嵌套查询的DSL
        :param path:
        :param level:
        :param cur_path:
        :param item_dsl:
        :return:
        """
        if level == 0:
            return {'nested': {'path': cur_path, 'query': item_dsl}}
        else:
            next_path = cur_path + '.' + path
            return {'nested': {'path': cur_path,
                               'query': self._get_nest_dsl(path, level - 1, next_path, item_dsl)}}

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
        elif '_' in sort and '_' != sort[0:1]:
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
        Sort=price:1; script:script(),type(),order();geodistance:location(48.8, 2.35),unit(km),order(0)
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
        return {self.global_id_to_field(sort_field_id): {"order": order, "unmapped_type": "double"}}

    def parse_basic_conditions(self, basic_conditions):
        """
        获取基础查询条件QDSL
        :param basic_condition:
        :return:
        """
        basic_condition_items = basic_conditions.split(';') if ';' in basic_conditions else basic_conditions.split('_')
        return chain(*map(lambda condition_item: self.parse_basic_condition_item(condition_item),
                          basic_condition_items)) if basic_conditions else []

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
        prop_condition_items = prop_conditions.split(';') if ';' in prop_conditions else prop_conditions.split('_')
        return chain(*map(lambda condition_item: self.parse_prop_condition_item(condition_item),
                          prop_condition_items)) if prop_conditions else []

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
        elif field_name == '\price':
            return 'price'
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
    def get_agg_qdl(self, es_config, index, type, query_params, parse_fields, es_connection=None):
        ignore_default_agg_str = get_dict_value(query_params, 'ignore_default_agg')
        ignore_default_agg = (ignore_default_agg_str.lower() == 'true')
        if not ignore_default_agg:
            cats = get_dict_value(query_params, 'cats')
            catpath_agg_qdl = self.parse_catpath_agg_condition(cats)
            # cur_agg_qdl = self.agg_dsl.copy().update(catpath_agg_qdl)
            cur_agg_qdl = dict(self.get_origin_agg_qdl(), **catpath_agg_qdl)
            if 'brand' in query_params:
                # 如果品牌在查询条件中，那么就表示不需要再对品牌做聚合
                cur_agg_qdl.pop('brand', None)
        else:
            cur_agg_qdl = {}
        qdsl = self.get_product_query_qdsl(es_config, index, type, query_params, parse_fields, es_connection)
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
        # suggest_size_multiple = config.get_value('/consts/suggest/tag_query_multiple') or 10
        cur_suggest_qdl['completion_suggest']['completion']['size'] = int(suggest_size)
        cur_suggest_qdl['completion_suggest']['completion']['context']['type'] = type
        return cur_suggest_qdl

    ###########################################################################################
    # Search QDSL解析相关函数 start
    ###########################################################################################
    def get_search_qdl(self, es_config, index, type, query_params, parse_fields, es_connection=None,
                       ignore_default_agg=False):
        res_str = get_dict_value(query_params, 'res')
        if not res_str:
            res_str = 'products,aggregations'
        res_list = map(lambda res: res.strip(), res_str.strip().split(','))

        qdsl = self.get_product_query_qdsl(es_config, index, type, query_params, parse_fields, es_connection)
        if 'aggregations' in res_list:
            ignore_default_agg_str = get_dict_value(query_params, 'ignore_default_agg')
            ignore_default_agg = ignore_default_agg or (ignore_default_agg_str.lower() == 'true')
            if not ignore_default_agg:
                cats = get_dict_value(query_params, 'cats')
                catpath_agg_qdl = self.parse_catpath_agg_condition(cats)
                cur_agg_qdl = dict(self.get_origin_agg_qdl(), **catpath_agg_qdl)
                if 'brand' in query_params:
                    # 如果品牌在查询条件中，那么就表示不需要再对品牌做聚合
                    cur_agg_qdl.pop('brand', None)
            else:
                cur_agg_qdl = {}
            qdsl['aggs'] = cur_agg_qdl
        qdsl = deep_merge(qdsl, extend_parser.get_agg_qdsl(query_params))
        if 'products' not in res_list:
            qdsl['size'] = 0

        return qdsl


qdsl_parser = QdslParser()
