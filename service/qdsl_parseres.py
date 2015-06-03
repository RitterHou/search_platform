# -*- coding: utf-8 -*-
import json
from itertools import *

from common.configs import config
from common.scripts import python_invoker
from common.utils import singleton, get_dict_value, unbind_variable, deep_merge
from common.loggers import debug_log, query_log


# from search_platform.settings import QUERY_PARAM_DEFAULT_VALUE as DEFAULT_VALUE
from search_platform.settings import ORDER_ID
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

        must_bool_query_qdsl_list = list(chain(self.parse_query_string_condition(query_string, es_connection),
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

    def parse_query_string_condition(self, query_string, es_connection):
        """
        获取搜索框字符查询QDSL
        :param query_string:
        :return:
        """
        if query_string:
            analyze_token_list = es_adapter.query_text_analyze_result_without_filter(es_connection, 'standard',
                                                                                     query_string)
            if analyze_token_list:
                return map(lambda analyze_token: {'match': {'_all': analyze_token}}, analyze_token_list)
        return [{'match': {'_all': query_string}} if query_string else {'match_all': {}}]

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
        return {'sort': [self.parse_sort_param_item(item) for item in sort.strip().split('_') if
                         item or '_' in item]} if sort and sort.strip() else None


    def parse_sort_param_item(self, sort_item):
        """
        单个排序字段进行处理，字段和顺序用","分割，4,1示例表示：销量升序
        :param sort_item:
        :return:
        """
        sort_field_id, sort_seq_id = sort_item.strip().split(':', 1)
        return {self.global_id_to_field(sort_field_id): self.order_num_to_es_str(sort_seq_id)}


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
        # return GLOBAL_ID_TO_FIELD[field_id] if field_id in GLOBAL_ID_TO_FIELD else ''
        return self.field_to_es_field(field_id)

    @staticmethod
    def order_num_to_es_str(order_num):
        """
        排序字段映射, 默认1代表升序，0代表降序
        :param order_num:
        :return:
        """
        return ORDER_ID[order_num] if order_num in ORDER_ID else 'asc'

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
        cur_suggest_qdl['completion_suggest']['completion']['size'] = suggest_size
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

        if 'products' not in res_list:
            qdsl['size'] = 0

        return qdsl


class ExtendQdslParser(object):
    """
    商品扩展查询支持
    """


    def __init__(self):
        self.regex = '(?P<op_type>[\\d\\D]+?)\\((?P<field_str>[\\d\\D]+?)\\)'
        self.dsl_regex = 'tmpl=(?P<tmpl>[\\d\\D]+?),param=(?P<param>[\\d\\D]+)'
        self.QUERY_QDSL_PARSER_DICT = {'term': self.__get_query_term_fragment, 'range': self.__get_query_range_fragment,
                                       'ids': self.__get_query_ids_fragment, 'match': self.__get_query_match_fragment,
                                       'querystring': self.__get_query_querystring_fragment}

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
        return reduce(deep_merge, (query_qdsl, dsl_qdsl, script_qdsl))

    @debug_log.debug('get_query_qdsl')
    def get_query_qdsl(self, query_params):
        """
        获取所有额外查询的qdsl
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.__get_query_params_by_prefix(query_params, 'ex_q_')
        qdsl_must_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_query_qdsl_list_fragment(field_name, filed_value_list),
            ex_query_params_dict.iteritems()))
        return {'query': {'bool': {'must': qdsl_must_list}}} if qdsl_must_list else {}

    def get_request_dsl_qdsl(self, query_params):
        """
        处理客户端请求中的DSL，输入格式为: ex_dsl :  tmpl={json},param={json}
        :param query_params:
        :return:
        """
        input_qdsl_param_str = query_params.get('ex_dsl')
        if not input_qdsl_param_str:
            return {}
        temp_name, dsl_tmpl = unbind_variable(self.regex, 'tmpl', input_qdsl_param_str)
        temp_name, param_str = unbind_variable(self.regex, 'param', input_qdsl_param_str)
        if not dsl_tmpl:
            return None
        if param_str:
            param_dict = json.loads(param_str)
            dsl_tmpl = dsl_tmpl.format(param_dict)
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


    def __get_query_params_by_prefix(self, query_params, prefix_str):
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
        return {
            "query_string": {
                "default_field": field_name,
                "query": field_str
            }
        }

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


    def __parse_single_input_str(self, single_query_str):
        """
        解析单个输入字符串
        :param single_query_str:
        :return:
        """
        term_value_strs = single_query_str.split(',')
        return map(self.__get_obj_by_desc, term_value_strs)

    def __parse_range_input_str(self, range_query_str):
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
            temps = range_single_input_str.split('-')
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


qdsl_parser = QdslParser()
extend_parser = ExtendQdslParser()
