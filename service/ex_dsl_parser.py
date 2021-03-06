# coding=utf-8
import json

import re

from algorithm.section_partitions import equal_section_partitions
from common.configs import config
from common.connections import Es7ConnectionFactory
from common.exceptions import InvalidParamError, GenericError
from common.loggers import query_log
from common.scripts import python_invoker
from common.utils import deep_merge, unbind_variable, get_dict_value
from search_platform import settings

__author__ = 'liuzhaoming'


class ExtendQdslParser(object):
    """
    通用文档扩展查询支持
    """

    def __init__(self):
        self.regex_field_str = '(?P<op_type>[\\d\\D]+?)\\((?P<field_str>[\\d\\D]*)\\)'
        self.dsl_regex_tmpl = 'tmpl:\\((?P<tmpl>[\\d\\D]+?)\\)'
        self.dsl_regex_param = 'param:\\((?P<param>[\\d\\D]+?)\\)'
        self.variable_name_pattern = re.compile(r'{[a-zA-Z$][a-zA-Z0-9_$]*?}')
        self.section_regex_optimize = 'optimize:(?P<optimize>[\\w]+)'
        self.section_regex_size = 'size:(?P<size>[\\d]+)'
        self.highlight_query_tmpl = {"number_of_fragments": 0, "highlight_query": {"bool": {"must": []}}}
        self.QUERY_QDSL_PARSER_DICT = {
            'term': self.__get_query_term_fragment,
            'range': self.__get_query_range_fragment,
            'date_range': self.__get_query_date_range_fragment,
            'terms': self.__get_query_term_fragment,
            'bterms': self.__get_query_bool_term_fragment,
            'size_terms': self.__get_query_size_term_fragment,
            'ids': self.__get_query_ids_fragment,
            'match': self.__get_query_match_fragment,
            'query_string': self.__get_query_querystring_fragment,
            'multi_match': self.__get_query_multi_match_fragment,
            'more_like_this': self.__get_query_more_like_this_fragment,
            'prefix': self.__get_query_prefix_fragment,
            'regexp': self.__get_query_regexp_fragment,
            'span_first': self.__get_query_span_first_fragment,
            'span_near': self.__get_query_span_near_fragment,
            'wildcard': self.__get_query_wildcard_fragment,
            'stock': self.__get_query_stock_fragment,
            'nested': self.__get_query_nested_fragment,
            'nested_multi': self.__get_query_nested_multi_fragment,
            'ematch': self.__get_query_ematch_fragment,
            'not': self.__get_query_not_fragment,
            'or': self.__get_query_or_fragment,
            'null': self.__get_query_null_fragment,
        }
        self.FILTER_QDSL_PARSER_DICT = {
            'geo_distance': self.__get_filter_geo_distance_fragment,
            'geo_distance_range': self.__get_filter_geo_distance_range_fragment,
            'geo_bounding_box': self.__get_filter_geo_bounding_box_fragment,
            'null': self.__get_filter_null_fragment,
        }
        self.AGGS_QDSL_PARSER_DICT = {
            'max': self.__get_agg_max_fragment,
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
            'term': self.__get_agg_terms_fragment,
            'range': self.__get_agg_range_fragment,
            'date_range': self.__get_agg_date_range_fragment,
            'histogram': self.__get_agg_histogram_fragment,
            'date_histogram': self.__get_agg_date_histogram_fragment,
            'geo_distance': self.__get_agg_geo_distance_fragment,
            'cats': self.__get_agg_cats_fragment,
            'key_value': self.__get_agg_key_value_fragment,
            'nested': self.__get_nested_agg_fragment,
            'named_sub': self.__get_agg_named_sub_fragment,
            'sub': self.__get_agg_sub_fragment,
        }

    def get_qdsl(self, query_params, es_config):
        """
        获取自定义查询的QDSL
        :param query_params:
        :return:
        """
        query_qdsl = self.get_query_qdsl(query_params, es_config)
        rescore_qdsl = self.get_rescore_qdsl(query_params, es_config)
        dsl_qdsl = self.get_request_dsl_qdsl(query_params, es_config)
        script_qdsl = self.get_request_py_script_qdsl(query_params, es_config)
        hight_qdsl = self.get_highlight_dsl(query_params, es_config)
        custom_hight_qdsl = self.get_custom_highlight_dsl(query_params, es_config)
        fields_qdsl = self.get_fields_dsl(query_params, es_config)
        script_fields_qdsl = self.get_script_fields_dsl(query_params, es_config)
        fielddata_fields_qdsl = self.get_fielddata_fields_dsl(query_params, es_config)
        filter_qdsl = self.get_filter_qdsl(query_params, es_config)
        return reduce(deep_merge, (
            query_qdsl, rescore_qdsl, dsl_qdsl, script_qdsl, hight_qdsl, fields_qdsl, script_fields_qdsl,
            fielddata_fields_qdsl, filter_qdsl, custom_hight_qdsl))

    def get_rescore_qdsl(self, query_params, es_config):
        """
        rescore用于对得到的结果根据条件进行重新排序
        :param query_params:
        :return:
        """
        field_str = query_params.get('rescore')
        if not field_str:
            return {}

        if field_str:
            search_item_str_list = field_str.split(';')
            values = {}
            for search_item_str in search_item_str_list:
                name, value = search_item_str.split(':')
                values[name] = value

            query_weight = float(values['query_weight']) if 'query_weight' in values else 1
            rescore_query_weight = float(values['rescore_query_weight']) if 'rescore_query_weight' in values else 1.5

            sub_queries = values['query'].split('|')
            sub_query_dsls = []
            for sub_query in sub_queries:
                _, sub_query = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', sub_query)
                item_name, item_value = sub_query.split('=', 1)
                item_query_dsl = self.get_query_qdsl_single_fragment(item_name[len('ex_q_'):], item_value, es_config)

                sub_query_dsl = {
                    "query": {
                        "query_weight": query_weight,
                        "rescore_query_weight": rescore_query_weight,
                        'rescore_query': item_query_dsl
                    }
                }
                sub_query_dsls.append(sub_query_dsl)
            return {'rescore': sub_query_dsls}

    def get_agg_qdsl(self, query_params, es_config):
        """
        获取自定义的聚合DSL
        :param query_params:
        :return:
        """
        # 价格自动分区aggs
        section_agg_statis_dsl = self.get_section_agg_statis_dsl(query_params, es_config)

        # 通用聚合
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_agg_')
        agg_qdsl_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_agg_qdsl_list_fragment(field_name, filed_value_list,
                                                                                   es_config),
            ex_query_params_dict.iteritems()))
        ex_aggs_dsl = {}
        for agg_dsl in agg_qdsl_list:
            ex_aggs_dsl.update(agg_dsl)

        return deep_merge({'aggs': ex_aggs_dsl}, section_agg_statis_dsl)

    def get_section_agg_statis_dsl(self, query_params, es_config):
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
        :param agg_es_result:
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
            std_deviation = 0 if std_deviation == 'NaN' else std_deviation
            if avg is None or std_deviation is None:
                continue
            section_num = int(unbind_variable(self.section_regex_size, 'size', value)[1] or '0')
            agg_range_list = equal_section_partitions.get_child_section_range_list(avg, std_deviation, section_num)
            agg_range_dsl = {
                'aggs': {field_name + '_range': {"range": {"ranges": agg_range_list, "field": field_name}}}}
            sum_agg_range_dsl = deep_merge(sum_agg_range_dsl, agg_range_dsl)
        return sum_agg_range_dsl

    def get_fields_dsl(self, query_params, es_config):
        """
        获取fields查询dsl，格式为：ex_fields=spuId,salePrice,skuId
        :param query_params:
        :param es_config:
        :return:
        """
        fields_str = query_params.get('ex_fields')
        exclude_fields_str = query_params.get('ex_exclude_fields')
        if not fields_str and not exclude_fields_str:
            return {}

        dsl = {'_source': {}}
        if fields_str and fields_str.split(','):
            if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
                dsl['_source']['includes'] = fields_str.split(',')
            else:
                dsl['_source']['include'] = fields_str.split(',')
        if exclude_fields_str and exclude_fields_str.split(','):
            if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
                dsl['_source']['excludes'] = exclude_fields_str.split(',')
            else:
                dsl['_source']['exclude'] = exclude_fields_str.split(',')
        return dsl

    def get_script_fields_dsl(self, query_params, es_config):
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

    def get_fielddata_fields_dsl(self, query_params, es_config):
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

    def get_highlight_dsl(self, query_params, es_config):
        """
        获取高亮查询DSL, url高亮的查询语法为：
        ex_highlight_title=highlight(q:kkkkyyy,pre_tags:<em>,post_tags:</em>,multi_field:title.standard)
        :param query_params:
        :return:
        """
        from service.dsl_parser import qdsl_parser

        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_highlight_')
        sum_highlight_dsl = {}
        global_query_string = get_dict_value(query_params, 'q')
        global_query_string_dsl = None
        es_connection = Es7ConnectionFactory.get_es_connection(host=settings.SERVICE_BASE_CONFIG['elasticsearch'])
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

    def get_custom_highlight_dsl(self, query_params, es_config):
        """
        获取自定义的高亮查询语法
        ex_custom_highlight_title=highlight(pre_tags:<em>,post_tags:</em>)
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_custom_highlight_')
        sum_highlight_dsl = {'highlight': {'fields': {}}}
        for (key, value_list) in ex_query_params_dict.iteritems():
            var_name, field_names = unbind_variable('ex_custom_highlight_(?P<field_names>[\\d\\D]+)', 'field_names',
                                                    key)
            temp, value = unbind_variable('\\((?P<value>[\\d\\D]+)\\)', 'value', value_list[0])
            if value:
                pre_tags = unbind_variable('pre_tags:(?P<pre_tags>[\\d\\D]+?),', 'pre_tags', value)[1] or \
                           unbind_variable('pre_tags:(?P<pre_tags>[\\d\\D]+)', 'pre_tags', value)[1] or '<hl>'
                post_tags = unbind_variable('post_tags:(?P<post_tags>[\\d\\D]+?),', 'post_tags', value)[1] or \
                            unbind_variable('post_tags:(?P<post_tags>[\\d\\D]+)', 'post_tags', value)[1] or '</hl>'
            else:
                pre_tags = '<hl>'
                post_tags = '</hl>'

            for field_name in field_names.split(','):
                sum_highlight_dsl['highlight']['fields'][field_name] = {
                    'pre_tags': [pre_tags],
                    'post_tags': [post_tags]
                }
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

    def get_query_qdsl(self, query_params, es_config):
        """
        获取所有额外查询的qdsl
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_q_')
        qdsl_must_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_query_qdsl_list_fragment(field_name, filed_value_list,
                                                                                     es_config),
            ex_query_params_dict.iteritems()))
        return {'query': {'bool': {'must': qdsl_must_list}}} if qdsl_must_list else {}

    def get_filter_qdsl(self, query_params, es_config):
        """
        filter在7.x版本中已经被完全废弃，只能把filter兼容的改为query
        获取所有filter查询的dsl
        :param query_params:
        :return:
        """
        ex_query_params_dict = self.get_query_params_by_prefix(query_params, 'ex_f_')
        qdsl_must_list = filter(lambda item: item, map(
            lambda (field_name, filed_value_list): self.get_filter_qdsl_list_fragment(field_name, filed_value_list,
                                                                                      es_config),
            ex_query_params_dict.iteritems()))
        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            # 因为elasticsearch7不再支持filtered，所以在这里用query进行替换兼容
            return {'query': {'bool': {'must': qdsl_must_list}}} if qdsl_must_list else {}
        else:
            return {
                'query': {
                    'bool': {
                        'must': [{
                            'filtered': {
                                'filter': {
                                    'bool': {
                                        'must': qdsl_must_list
                                    }
                                }
                            }
                        }]
                    }
                }
            } if qdsl_must_list else {}

    def upgrade_ex_dsl(self, ex_dsl):
        """
        更新查询elasticsearch的原生dsl，以实现对新版本elasticsearch-7.x的兼容
        :param ex_dsl:
        :return:
        """

        def get_nested_dict(keys, value):
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            return value

        def upgrade_filter_missing(key, value):
            """
            更新filtered的存在检测dsl
            :param key:
            :param value:
            :return:
            """
            if key == 'filtered':
                field_name_missing = get_nested_dict(['filter', 'missing', 'field'], value)
                if field_name_missing:
                    return {"must_not": {"exists": {"field": field_name_missing}}}

                field_name_exists = get_nested_dict(['filter', 'exists', 'field'], value)
                if field_name_exists:
                    return {"must": {"exists": {"field": field_name_exists}}}

        def del_match_type(key, value):
            """
            删除match查询中的type字段
            :param key:
            :param value:
            :return:
            """
            if key == 'match' and isinstance(value, dict):
                values = value.values()
                if len(values) > 0 and isinstance(values[0], dict) and 'type' in values[0]:
                    del values[0]['type']

        def upgrade(dsl):
            """
            更新dsl
            :param dsl:
            :return:
            """
            if isinstance(dsl, dict):
                for key, value in dsl.iteritems():
                    del_match_type(key, value)
                    dsl_missing = upgrade_filter_missing(key, value)
                    if dsl_missing:
                        # 移除之前的空过滤查询语句
                        del dsl['filtered']
                        # 设置新的过滤查询语句
                        dsl['bool'] = dsl_missing
                    upgrade(value)
            elif isinstance(dsl, list):
                for value in dsl:
                    upgrade(value)

        upgrade(ex_dsl)

        return ex_dsl

    def get_request_dsl_qdsl(self, query_params, es_config):
        """
        处理客户端请求中的DSL，输入格式为: ex_dsl :  tmpl={json},param={json}
        示例：GET /view/retail/trades/?ex_dsl=tmpl:({"query":{"bool":{"must":[{"term":{"tid":"{tid}"}}]}}}),param:({"tid":"TC18110611104025875470"})&ex_fields=tid
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
        dsl = json.loads(dsl_tmpl)

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            # 处理DSL在elasticsearch7中的兼容性问题
            dsl = self.upgrade_ex_dsl(dsl)

        return dsl

    def get_request_py_script_qdsl(self, query_params, es_config):
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

    def get_query_qdsl_list_fragment(self, field_name, filed_value_list, es_config):
        """
        获取符合查询条件QDSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_q_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_query_qdsl_single_fragment(field_name, input_str,
                                                                                              es_config),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return {'bool': {'should': qdsl_fragment_list, 'minimum_should_match': 1}}

    def get_filter_qdsl_list_fragment(self, field_name, filed_value_list, es_config):
        """
        获取符合过滤条件QDSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_f_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_filter_qdsl_single_fragment(field_name, input_str,
                                                                                               es_config),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return {'bool': {'should': qdsl_fragment_list, 'minimum_should_match': 1}}

    def get_agg_qdsl_list_fragment(self, field_name, filed_value_list, es_config):
        """
        获取符合聚合DSL，前台获取到是参数list
        :param field_name:
        :param filed_value_list:
        :return:
        """
        field_name = field_name[len('ex_agg_'):]
        qdsl_fragment_list = filter(lambda item: item,
                                    map(lambda input_str: self.get_agg_qdsl_single_fragment(field_name, input_str,
                                                                                            es_config),
                                        filed_value_list))
        if len(qdsl_fragment_list) == 1:
            return qdsl_fragment_list[0]
        elif len(qdsl_fragment_list) > 1:
            return reduce(deep_merge, qdsl_fragment_list)

    def get_query_qdsl_single_fragment(self, field_name, input_str, es_config):
        """
        获取单个查询条件QDSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex_field_str, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex_field_str, 'field_str', input_str)
        if field_str is None:
            # query_log.info('Get query qdsl fragment the field_str is null')
            return None
        if op_type not in self.QUERY_QDSL_PARSER_DICT:
            query_log.warning('Get query qdsl fragment has not support op type {0}', op_type)
            return None
        return self.QUERY_QDSL_PARSER_DICT[op_type](field_name, field_str, es_config)

    def get_filter_qdsl_single_fragment(self, field_name, input_str, es_config):
        """
        获取单个过滤条件QDSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex_field_str, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex_field_str, 'field_str', input_str)
        field_str = field_str or ''
        if op_type not in self.FILTER_QDSL_PARSER_DICT:
            query_log.warning('Get filter qdsl fragment has not support op type {0}', op_type)
            return None
        return self.FILTER_QDSL_PARSER_DICT[op_type](field_name, field_str, es_config)

    def get_agg_qdsl_single_fragment(self, field_name, input_str, es_config):
        """
        获取单个聚合DSL
        :param field_name:
        :param input_str:
        :return:
        """
        temp_name, op_type = unbind_variable(self.regex_field_str, 'op_type', input_str)
        temp_name, field_str = unbind_variable(self.regex_field_str, 'field_str', input_str)
        field_str = field_str or ''
        if op_type not in self.AGGS_QDSL_PARSER_DICT:
            query_log.warning('Get agg qdsl fragment has not support op type {0}', op_type)
            return None
        return self.AGGS_QDSL_PARSER_DICT[op_type](field_name, field_str, es_config)

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

    def __get_query_bool_term_fragment(self, field_name, field_str, es_config):
        """
        针对ES 2.0版本terms不再支持minimum_should_match属性，改用bool查询进行拼接
        支持字段null查询，null用'\null\'表示
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_bool_term_item_query(_term_value):
            if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7' and not _term_value:
                return {"bool": {"must_not": {"exists": {"field": field_name}}}}

            if _term_value == '\\null\\':
                return {
                    "filtered": {
                        "filter": {
                            "missing": {
                                "field": field_name
                            }
                        }
                    }
                }
            else:
                return {"term": {field_name: _term_value}}

        term_values = self.__parse_single_input_str(field_str)
        term_item_query_dsl_list = map(parse_bool_term_item_query, term_values)
        return {
            "bool": {
                "should": term_item_query_dsl_list,
                "minimum_should_match": 1
            }
        }

    def __get_query_term_fragment(self, field_name, field_str, es_config):
        """
        term查询解析
        :param field_name:
        :param field_str:
        :return:
        """
        term_values = self.__parse_single_input_str(field_str)

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            terms = []
            for value in list(term_values):
                terms.append({"term": {field_name: value}})
            return {
                "bool": {
                    "minimum_should_match": 1,
                    "should": terms
                }
            }
        else:
            return {
                "terms": {
                    field_name: list(term_values),
                    "minimum_should_match": 1
                }
            }

    def __get_query_size_term_fragment(self, field_name, field_str, es_config):
        """
        terms查询，可以指定匹配条件数目, 不指定size参数或者指定size为0表示全匹配,
        ex_q_kkkk=size_terms(value:a,b,c;size:0)
        :param field_name:
        :param field_str:
        :return:
        """
        search_item_str_list = field_str.split(';')
        term_values = []
        term_size = 0
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'value':
                    term_values = self.__parse_single_input_str(search_item_key_value[1])
                elif search_item_key_value[0] == 'size':
                    term_size = int(search_item_key_value[1])

        if term_size > len(list(term_values)) or term_size == 0:
            term_size = len(list(term_values))

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            terms = []
            for value in list(term_values):
                terms.append({"term": {field_name: value}})
            return {
                "bool": {
                    "minimum_should_match": term_size,
                    "should": terms
                }
            }
        else:
            return {
                "terms": {
                    field_name: list(term_values),
                    "minimum_should_match": term_size
                }
            }

    def __get_query_ids_fragment(self, field_name, field_str, es_config):
        """
        ids查询
        :param field_name:
        :param field_str:
        :return:
        """
        id_values = self.__parse_single_input_str(field_str)
        return {
            "ids": {
                "values": list(id_values),
            }
        }

    def __get_query_range_fragment(self, field_name, field_str, es_config):
        """
        范围查询
        :param field_name:
        :param field_str:
        :return:
        """

        range_values = self.__parse_range_input_str(field_str)
        range_qdsl_list = map(lambda (floor_value, ceiling_value): {
            "range": {field_name: self.__range_value_to_qdsl(floor_value, ceiling_value)}}, range_values)

        return {'bool': {'should': range_qdsl_list, 'minimum_should_match': 1}}

    def __get_query_date_range_fragment(self, field_name, field_str, es_config):
        """
        时间范围查询,时间格式:"2012-01-01" "2012-01-01T00:00:00+01:00" "2011-12-31T23:00:00" "now"
        为了和时间的"-"区分,区间的分隔符为"--"
        :param field_name:
        :param field_str:
        :return:
        """
        range_values = self.__parse_range_input_str(field_str, split_char='--')
        range_qdsl_list = map(lambda (floor_value, ceiling_value): {
            "range": {field_name: self.__range_value_to_qdsl(floor_value, ceiling_value)}}, range_values)
        if range_qdsl_list:
            return {'bool': {'should': range_qdsl_list, 'minimum_should_match': 1}}
        return None

    def __range_value_to_qdsl(self, floor_value, ceiling_value):
        range_qsdl_item = {}
        if floor_value is not None:
            range_qsdl_item['gte'] = floor_value
        if ceiling_value is not None:
            range_qsdl_item['lt'] = ceiling_value
        return range_qsdl_item

    def __get_query_querystring_fragment(self, field_name, field_str, es_config):
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

    def __get_query_match_fragment(self, field_name, field_str, es_config):
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

    def __get_query_ematch_fragment(self, field_name, field_str, es_config):
        """
        ematch(扩展match) 查询
        格式为：ex_q_kkk=ematch(query:abcdefg;minimum_should_match:2;operator:and)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_str:
            return None

        match_dsl = {}
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'minimum_should_match':
                    match_dsl[search_item_key_value[0]] = int(search_item_key_value[1])
                else:
                    match_dsl[search_item_key_value[0]] = search_item_key_value[1]
        if 'query' not in match_dsl:
            # 没有match关键词，抛出异常
            return {}

        return {"match": {field_name: match_dsl}}

    def __get_query_stock_fragment(self, field_name, field_str, es_config):
        """
        获取库存查询接口，库存查询参数为：
        ex_q_stock=stock(range:-10,20-60,70-;region:nanjing,field:stock)，
        如果不带region参数表示是全部库存，带region表示区域库存
        field字段表示查询库存数目字段，默认值为stock
        :param field_name:
        :param field_str:
        :return:
        """
        search_item_str_list = field_str.split(';')
        region = None
        range_str = None
        stock_num_field = 'stock'
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')

            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'range':
                    range_str = search_item_key_value[1]
                elif search_item_key_value[0] == 'region':
                    region = search_item_key_value[1]
                elif search_item_key_value[0] == 'field':
                    stock_num_field = search_item_key_value[1]

        if not range_str:
            return {}

        if not region:
            # 表示是重量库存
            range_dsl = self.__get_query_range_fragment('{0}.{1}'.format(field_name, stock_num_field), range_str,
                                                        es_config)
            return {"nested": {"path": field_name, "query": range_dsl}}
        else:
            # 表示区域库存
            range_dsl = self.__get_query_range_fragment('{0}.regions.{1}'.format(field_name, stock_num_field),
                                                        range_str, es_config)
            return {
                "nested": {"path": field_name,
                           "query": {"nested": {"path": field_name + '.regions', "query": range_dsl}}}}

    def __get_query_nested_fragment(self, field_name, field_str, es_config):
        """
        嵌套文档查询
        ex_q_kkk=nested(field:a|b;query:<>)
        field：如果中间有多层路径则放在path中,用“|”分隔
        query：存放嵌套查询的实体查询
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_query_value(_query_str):
            if not _query_str:
                return None
            _, _nested_item_query_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _query_str)
            return _nested_item_query_str

        def compute_nested_path_dsl(_path_fields, level, _nested_item_query_dsl):
            """
            计算多层嵌套文档查询DSL
            """
            if level < len(_path_fields):
                cur_path_field_str = '.'.join(path_fields[0:level])
                return {
                    "nested": {"path": cur_path_field_str,
                               "query": compute_nested_path_dsl(_path_fields, level + 1, _nested_item_query_dsl)}}
            elif level == len(_path_fields):
                return _nested_item_query_dsl

        search_item_str_list = field_str.split(';')
        path_fields = None
        nested_item_query_str = None
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':')

            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'field':
                    path_str = search_item_key_value[1]
                    path_fields = path_str.split('|')
                elif search_item_key_value[0] == 'query':
                    query_str = search_item_key_value[1]
                    nested_item_query_str = parse_query_value(query_str)

        if not nested_item_query_str or not path_fields:
            return {}
        path_fields = [field_name] + path_fields
        nested_item_query_dsl = self.get_query_qdsl_single_fragment('.'.join(path_fields), nested_item_query_str,
                                                                    es_config)

        return compute_nested_path_dsl(path_fields, 1, nested_item_query_dsl)

    def __get_query_nested_multi_fragment(self, field_name, field_str, es_config):
        """
        嵌套文档多个子查询，只支持一级子查询
        ex_q_userInfo=nested_multi(field:gender;query:<terms(未知)>|field:birthday;query:<range(-1551756022810)>)
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_query_value(_query_str):
            if not _query_str:
                return None
            _, _nested_item_query_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _query_str)
            return _nested_item_query_str

        sub_queries = []
        for sub_query_str in field_str.split('|'):
            for search_item_key_values in sub_query_str.split(';'):
                search_item_key_value = search_item_key_values.split(':')
                if search_item_key_value[0] == 'field':
                    field = search_item_key_value[1]
                elif search_item_key_value[0] == 'query':
                    query_str = search_item_key_value[1]
                    nested_item_query_str = parse_query_value(query_str)

            if not field or not nested_item_query_str:
                return {}

            nested_item_query_dsl = self.get_query_qdsl_single_fragment(field_name + '.' + field,
                                                                        nested_item_query_str, es_config)
            sub_queries.append(nested_item_query_dsl)
        return {"nested": {"path": field_name, "query": {"bool": {"must": sub_queries}}}}

    def __get_query_multi_match_fragment(self, field_name, field_str, es_config):
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

    def __get_query_more_like_this_fragment(self, field_name, field_str, es_config):
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

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            more_like_this_dsl['like'] = more_like_this_dsl.pop('like_text')

        return {"more_like_this": more_like_this_dsl}

    def __get_query_prefix_fragment(self, field_name, field_str, es_config):
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

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            value = prefix_dsl[field_name]
            prefix_value = value.get('value') or value.get('prefix')
            prefix_dsl = {field_name: prefix_value}

        return {"prefix": prefix_dsl}

    def __get_query_regexp_fragment(self, field_name, field_str, es_config):
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

    def __get_query_span_first_fragment(self, field_name, field_str, es_config):
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

    def __get_query_span_near_fragment(self, field_name, field_str, es_config):
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

    def __get_query_wildcard_fragment(self, field_name, field_str, es_config):
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

    def __get_filter_geo_distance_fragment(self, field_name, field_str, es_config):
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

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            location, distance = None, None
            if field_str:
                search_item_str_list = field_str.split(';')
                for search_item_str in search_item_str_list:
                    search_item_key_value = search_item_str.split(':')
                    if len(search_item_key_value) > 1:
                        if search_item_key_value[0] == 'location':
                            location = search_item_key_value[1]
                        elif search_item_key_value[0] == 'distance':
                            distance = search_item_key_value[1]

            if location is None or distance is None:
                raise InvalidParamError('Location or distance should\'t be null')
            return {"bool": {"filter": {"geo_distance": {"distance": distance, field_name: location}}}}
        else:
            search_item_str_list = field_str.split(';')
            geo_distance_filter_dsl = {field_name: {}}
            for search_item_str in search_item_str_list:
                search_item_key_value = search_item_str.split(':')
                if len(search_item_key_value) > 1:
                    if search_item_key_value[0] == 'boost':
                        geo_distance_filter_dsl[search_item_key_value[0]] = float(search_item_key_value[1])
                    elif search_item_key_value[0] == 'location':
                        geo_distance_filter_dsl[field_name] = str(search_item_key_value[1])
                    else:
                        geo_distance_filter_dsl[search_item_key_value[0]] = search_item_key_value[1]
            return {"geo_distance": geo_distance_filter_dsl}

    def __get_filter_geo_distance_range_fragment(self, field_name, field_str, es_config):
        """
        距离范围查询
        Geo Distance Range Filter
        Ex_f_distance=geo_distance_range(location:12.0,2.35;from:100km;to:500km)
        :param field_name:
        :param field_str:
        :return:
        """
        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            raise GenericError('<ex_f_distance=geo_distance_range()> not support elasticsearch7')

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
                    geo_distance_filter_dsl[field_name] = str(search_item_key_value[1])
                else:
                    geo_distance_filter_dsl[search_item_key_value[0]] = search_item_key_value[1]
        return {"geo_distance_range": geo_distance_filter_dsl}

    def __get_filter_geo_bounding_box_fragment(self, field_name, field_str, es_config):
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

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            top_left, bottom_right = None, None
            if field_str:
                search_item_str_list = field_str.split(';')
                for search_item_str in search_item_str_list:
                    search_item_key_value = search_item_str.split(':')
                    if len(search_item_key_value) > 1:
                        if search_item_key_value[0] == 'top_left':
                            top_left = search_item_key_value[1]
                        elif search_item_key_value[0] == 'bottom_right':
                            bottom_right = search_item_key_value[1]

            if top_left is None or bottom_right is None:
                raise InvalidParamError('Top left or bottom right should\'t be null')
            return {"bool": {"filter": {
                "geo_bounding_box": {field_name: {"top_left": top_left, "bottom_right": bottom_right}}}}}
        else:
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

    def __get_filter_null_fragment(self, field_name, field_str, es_config):
        """
        null 过滤，格式为：ex_f_属性名=null(flag:false),
        false主要是决定是null还是not null， true表示是null查询；false表示not null查询。默认为false
        根据user字段查询：
        返回true的情况：{ "user": "jane" }{ "user": "" } { "user": "-" } { "user": ["jane"] } { "user": ["jane", null ] }
        返回false的情况：{ "user": null }{ "user": [] } { "user": [null] } { "foo":  "bar" }
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        flag = False
        if field_str:
            search_item_str_list = field_str.split(';')
            for search_item_str in search_item_str_list:
                search_item_key_value = search_item_str.split(':')
                if len(search_item_key_value) > 1 and search_item_key_value[0] == 'flag':
                    flag = search_item_key_value[1].lower() == 'true'

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            if flag:
                return {"bool": {"must_not": {"exists": {"field": field_name}}}}
            else:
                return {"bool": {"must": {"exists": {"field": field_name}}}}
        else:
            if flag:
                return {"missing": {"field": field_name}}
            else:
                return {"exists": {"field": field_name}}

    def __get_query_not_fragment(self, field_name, field_str, es_config):
        """
        not查询
        ex_q_not=not(query:<>)
        query：存放not实体查询
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_query_value(_query_str):
            if not _query_str:
                return None
            _, _nested_item_query_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _query_str)
            return _nested_item_query_str

        search_item_str_list = field_str.split(';')
        item_query_str_list = []
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)

            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'query':
                    query_str_list = search_item_key_value[1].split('|')
                    for query_str in query_str_list:
                        cur_item_query_str = parse_query_value(query_str)
                        if cur_item_query_str:
                            item_query_str_list.append(cur_item_query_str)

        if not item_query_str_list:
            return {}

        item_query_dsl_list = []
        for item_query_str in item_query_str_list:
            field_name, field_str = item_query_str.split('=', 1)
            field_name = field_name[len('eq_q_'):]
            item_query_dsl = self.get_query_qdsl_single_fragment(field_name, field_str, es_config)
            item_query_dsl_list.append(item_query_dsl)

        return {'bool': {'must_not': item_query_dsl_list}}

    def __get_query_or_fragment(self, field_name, field_str, es_config):
        """
        or查询
        ex_q_or=or(query:<>)
        query：存放or实体查询
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_query_value(_query_str):
            if not _query_str:
                return None
            _, _nested_item_query_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _query_str)
            return _nested_item_query_str

        item_query_str_list = []
        search_item_key_value = field_str.split(':', 1)

        if len(search_item_key_value) > 1:
            if search_item_key_value[0] == 'query':
                query_str_list = search_item_key_value[1].split('|')
                for query_str in query_str_list:
                    cur_item_query_str = parse_query_value(query_str)
                    if cur_item_query_str:
                        item_query_str_list.append(cur_item_query_str)

        if not item_query_str_list:
            return {}

        item_query_dsl_list = []
        for item_query_str in item_query_str_list:
            field_name, field_str = item_query_str.split('=', 1)
            field_name = field_name[len('eq_q_'):]
            item_query_dsl = self.get_query_qdsl_single_fragment(field_name, field_str, es_config)
            item_query_dsl_list.append(item_query_dsl)

        return {'bool': {'should': item_query_dsl_list}}

    def __get_query_null_fragment(self, field_name, field_str, es_config):
        """
        null 查询，使用filtered query实现, 功能同 __get_filter_null_fragment
        格式为：ex_q_属性名=null(flag:false),
        false主要是决定是null还是not null， true表示是null查询；false表示not null查询。默认为false
        根据user字段查询：
        返回true的情况：{ "user": "jane" }{ "user": "" } { "user": "-" } { "user": ["jane"] } { "user": ["jane", null ] }
        返回false的情况：{ "user": null }{ "user": [] } { "user": [null] } { "foo":  "bar" }
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        flag = False
        if field_str:
            search_item_str_list = field_str.split(';')
            for search_item_str in search_item_str_list:
                search_item_key_value = search_item_str.split(':')
                if len(search_item_key_value) > 1 and search_item_key_value[0] == 'flag':
                    flag = search_item_key_value[1].lower() == 'true'

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            if flag:
                return {"bool": {"must_not": {"exists": {"field": field_name}}}}
            else:
                return {"bool": {"must": {"exists": {"field": field_name}}}}
        else:
            if flag:
                return {"filtered": {"filter": {"missing": {"field": field_name}}}}
            else:
                return {"filtered": {"filter": {"exists": {"field": field_name}}}}

    def __get_agg_max_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_min_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_sum_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_avg_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_stats_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_exstats_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_value_count_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_percentiles_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_percentile_ranks_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_cardinality_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_missing_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_terms_fragment(self, field_name, field_str, es_config):
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

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            if 'size' in agg_terms_dsl['terms'] and int(agg_terms_dsl['terms']['size']) == 0:
                # 参考：https://github.com/elastic/elasticsearch/issues/18838
                agg_terms_dsl['terms']['size'] = 2147483647

        return {aggs_key: agg_terms_dsl}

    def __get_agg_range_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_date_range_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_histogram_fragment(self, field_name, field_str, es_config):
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

    def __get_agg_date_histogram_fragment(self, field_name, field_str, es_config):
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
                    agg_date_histogram_dsl['date_histogram']['keyed'] = bool(search_item_key_value[1])
                elif search_item_key_value[0] == 'offset':
                    agg_date_histogram_dsl['date_histogram']['offset'] = search_item_key_value[1]
                else:
                    agg_date_histogram_dsl['date_histogram'][search_item_key_value[0]] = search_item_key_value[1]

        return {aggs_key: agg_date_histogram_dsl}

    def __get_agg_geo_distance_fragment(self, field_name, field_str, es_config):
        """
        Geo Distance Aggregation
        地理距离统计
        Ex_agg_location=geo_distance(ranges:num:45-num:666,num:99-,-num:200;origin:52.3760, 4.894;unit:km;
        distance_type:sloppy_arc;min_doc_count:10)
        :param field_name:
        :param field_str:
        :return:
        """

        def get_ranges_dict(floor_value, ceiling_value):
            """
            获取from to的dict
            """
            result = {}
            if floor_value is not None:
                result['from'] = floor_value
            if ceiling_value is not None:
                result['to'] = ceiling_value
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
                        lambda (floor_value, ceiling_value): get_ranges_dict(floor_value, ceiling_value), range_list)
                    if range_item_list:
                        agg_date_range_dsl['geo_distance']['ranges'] = range_item_list
                else:
                    agg_date_range_dsl['geo_distance'][search_item_key_value[0]] = search_item_key_value[1]

        if es_config.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            agg_date_range_dsl['geo_distance'].pop('min_doc_count')
            agg_date_range_dsl['geo_distance'].pop('distance_type')

        return {aggs_key: agg_date_range_dsl}

    def __get_agg_cats_fragment(self, field_name, field_str, es_config):
        """
        聚合商品类目路径，一次聚合出所有层级的类目
        ex_agg_cats=cats(depth:3)
        :param field_name:
        :param field_str:
        :return:
        """
        from service.dsl_parser import qdsl_parser

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

    def __get_agg_key_value_fragment(self, field_name, field_str, es_config):
        """
        key value聚合，聚合的数据格式为：
        "specs": [
                    {
                        "specialPropName": "睡袋类型",
                        "specialValName": "羽绒睡袋"
                    }
                ]
        ex_agg_specs=key_value(key:specialPropName;value:specialValName)
        :param field_name:
        :param field_str:
        :return:
        """
        if not field_name:
            return None
        aggs_key = 'ex_agg_' + field_name + '.key_value'
        key_field_name, value_field_name = None, None
        search_item_str_list = field_str.split(';')
        for search_item_str in search_item_str_list:
            search_item_key_value = search_item_str.split(':', 1)
            if len(search_item_key_value) > 1:
                if search_item_key_value[0] == 'key':
                    key_field_name = search_item_key_value[1]
                elif search_item_key_value[0] == 'value':
                    value_field_name = search_item_key_value[1]
        if not key_field_name or not value_field_name:
            return {}

        return {aggs_key: {
            "terms": {
                "script": "doc['" + field_name + "." + key_field_name + "'].value+'*##*'+doc['" + field_name + "."
                          + value_field_name + "'].value",
                "size": 0
            }}
        }

    def __get_agg_sub_fragment(self, field_name, field_str, es_config):
        """
        sub Aggregation，多层子聚合
        ex_q_sub=sub(aggs:<ex_agg_spuId=terms()>|<ex_agg_skuId=terms()>)
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_aggs_value(_agg_str):
            if not _agg_str:
                return None
            _, _nested_item_agg_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _agg_str)
            return _nested_item_agg_str

        if not field_name:
            return None
        agg_item_str_list = field_str.split(';')
        item_agg_str_list = []
        for agg_item_str in agg_item_str_list:
            agg_item_key_value = agg_item_str.split(':', 1)
            if len(agg_item_key_value) > 1:
                if agg_item_key_value[0] == 'aggs':
                    agg_str_list = agg_item_key_value[1].split('|')
                    for agg_str in agg_str_list:
                        cur_item_agg_str = parse_aggs_value(agg_str)
                        if cur_item_agg_str:
                            item_agg_str_list.append(cur_item_agg_str)
        if not item_agg_str_list:
            return {}
        item_agg_dsl_list = []
        for item_agg_str in item_agg_str_list:
            field_name, field_str = item_agg_str.split('=')
            field_name = field_name[len('eq_agg_'):]
            item_agg_dsl = self.get_agg_qdsl_single_fragment(field_name, field_str, es_config)
            item_agg_dsl_list.append(item_agg_dsl)
        cur_agg_dsl = None
        item_agg_dsl_list.reverse()
        for item_agg_dsl in item_agg_dsl_list:
            if not cur_agg_dsl:
                cur_agg_dsl = item_agg_dsl
            else:
                item_agg_dsl.values()[0]['aggs'] = cur_agg_dsl
                cur_agg_dsl = item_agg_dsl
        return cur_agg_dsl

    def __get_agg_named_sub_fragment(self, field_name, field_str, es_config):
        """
        named sub Aggregation，包含了自定义命名的多层子聚合
        ex_agg_name=named_sub(aggs:<ex_agg_spuId=terms()>|<ex_agg_skuId=terms()>)
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_aggs_value(_agg_str):
            if not _agg_str:
                return None
            _, _nested_item_agg_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _agg_str)
            return _nested_item_agg_str

        if not field_name:
            return None
        agg_item_str_list = field_str.split(';')
        item_agg_str_list = []
        for agg_item_str in agg_item_str_list:
            agg_item_key_value = agg_item_str.split(':', 1)
            if len(agg_item_key_value) > 1:
                if agg_item_key_value[0] == 'aggs':
                    agg_str_list = agg_item_key_value[1].split('|')
                    for agg_str in agg_str_list:
                        cur_item_agg_str = parse_aggs_value(agg_str)
                        if cur_item_agg_str:
                            item_agg_str_list.append(cur_item_agg_str)
        if not item_agg_str_list:
            return {}
        item_agg_dsl_list = []
        for item_agg_str in item_agg_str_list:
            item_field_name, item_field_str = item_agg_str.split('=')
            item_field_name = item_field_name[len('eq_agg_'):]
            item_agg_dsl = self.get_agg_qdsl_single_fragment(item_field_name, item_field_str, es_config)
            item_agg_dsl_list.append(item_agg_dsl)
        cur_agg_dsl = None
        item_agg_dsl_list.reverse()
        for item_agg_dsl in item_agg_dsl_list:
            if not cur_agg_dsl:
                cur_agg_dsl = item_agg_dsl
            else:
                item_agg_dsl.values()[0]['aggs'] = cur_agg_dsl
                cur_agg_dsl = item_agg_dsl
        return {'ex_agg_' + field_name + '_sub': cur_agg_dsl.values()[0]}

    def __get_nested_agg_fragment(self, field_name, field_str, es_config):
        """
        nested aggregation，嵌套类型的聚合
        ex_agg_payReceiptRecords=nested(aggs:<ex_agg_uniPaymentChannel=terms()>|<ex_agg_payAmount=sum()>)
        :param field_name:
        :param field_str:
        :return:
        """

        def parse_aggs_value(_agg_str):
            if not _agg_str:
                return None
            _, _nested_item_agg_str = unbind_variable(r'<(?P<value>[\d\D]+?)>', 'value', _agg_str)
            return _nested_item_agg_str

        if not field_name:
            return None
        agg_item_str_list = field_str.split(';')
        item_agg_str_list = []
        for agg_item_str in agg_item_str_list:
            agg_item_key_value = agg_item_str.split(':', 1)
            if len(agg_item_key_value) > 1:
                if agg_item_key_value[0] == 'aggs':
                    agg_str_list = agg_item_key_value[1].split('|')
                    for agg_str in agg_str_list:
                        cur_item_agg_str = parse_aggs_value(agg_str)
                        if cur_item_agg_str:
                            item_agg_str_list.append(cur_item_agg_str)
        if not item_agg_str_list:
            return {}
        item_agg_dsl_list = []
        for item_agg_str in item_agg_str_list:
            item_field_name, item_field_str = item_agg_str.split('=')
            item_field_name = item_field_name[len('eq_agg_'):]
            item_field_name = field_name + '.' + item_field_name  # 子字段前面加上field_name
            item_agg_dsl = self.get_agg_qdsl_single_fragment(item_field_name, item_field_str, es_config)
            item_agg_dsl_list.append(item_agg_dsl)
        cur_agg_dsl = None
        item_agg_dsl_list.reverse()
        for item_agg_dsl in item_agg_dsl_list:
            if not cur_agg_dsl:
                cur_agg_dsl = item_agg_dsl
            else:
                item_agg_dsl.values()[0]['aggs'] = cur_agg_dsl
                cur_agg_dsl = item_agg_dsl

        return {
            'ex_agg_' + field_name + '_nested': {
                'nested': {
                    'path': field_name
                },
                'aggs': cur_agg_dsl
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
            return self.__get_obj_by_desc(temps[0], split_char == '-') \
                , self.__get_obj_by_desc(temps[1], split_char == '-')

        term_value_strs = range_query_str.split(',')
        return filter(lambda (floor_value, ceiling_value): floor_value is not None or ceiling_value is not None,
                      map(__parse_single_range_str, term_value_strs))

    def __get_obj_by_desc(self, desc_str, with_type=True):
        """
        根据对象描述字符串生成对象，格式为: 'num:122.5', 'bool:false', 'str:kkwq'
        :param desc_str:
        :param with_type:
        :return:
        """
        if not desc_str:
            return None
        if not with_type:
            return desc_str

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
            if value_str[0] == '~':
                return 0 - float(value_str[1:])
            else:
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


extend_parser = ExtendQdslParser()
