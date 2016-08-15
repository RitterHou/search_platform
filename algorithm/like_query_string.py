# coding=utf-8
from re import match

from common.adapter import es_adapter

from common.configs import config
from common.loggers import app_log
from common.pingyin_utils import pingyin_utils
from common.utils import get_default_es_host
from service.dsl_parser import qdsl_parser


__author__ = 'liuzhaoming'


class LikeQueryString(object):
    """
    相似关键词查找算法，可用于搜索关键词纠错、建议等场景
    商品搜索智能纠错算法：
    1. 对于中文词，主要是纠正同音字错误的情况，同音字包含多音字，
    2. 对于英文，主要是纠正一个字母拼写错误的情况
    算法步骤：
    1. 获取搜索关键词，对关键词进行判断，区分中英文
    2. 如果是纯中文字符串，获取拼音，然后进行suggest查询，不支持混音（zh z , sh s, ch c, ）
    3. 无论中文、英文、中英文字符串，对name字段进行fuzzy query 查询
    """
    # 中文字符串
    chinese_characters_regex = u'^[\u4e00-\u9fa5]+$'


    def get_like_query_string(self, query_str, suggest_index_cfg):
        """
        获取和查询字符串相似的关键词
        :param query_str:
        :param suggest_index_cfg:
        :return:
        """
        if not query_str or not suggest_index_cfg or not suggest_index_cfg.get(
                'index') or not suggest_index_cfg.get('type'):
            app_log.warning(
                'LikeQueryString get_like_query_string invalid input params : '
                'origin_query_str={0}, suggest_index_cfg={1}', query_str, suggest_index_cfg)
            return ''

        if 'host' not in suggest_index_cfg:
            suggest_index_cfg['host'] = get_default_es_host()

        # 判断是否是纯中文字符串
        query_str = query_str.strip()
        like_str_list = []
        is_chinese_characters = match(self.chinese_characters_regex, query_str)
        if is_chinese_characters:
            like_str_list.extend(self.__get_homonym_query_result(query_str, suggest_index_cfg))

        like_str_list.extend(self.__get_fuzzy_query_result(query_str, suggest_index_cfg))
        return self.__merge_like_str_list(like_str_list, query_str, suggest_index_cfg)

    def __get_homonym_query_result(self, query_str, suggest_index_cfg):
        """
        中文同音词查询
        :param query_str:
        :return:
        """
        pingyin_str_list = pingyin_utils.get_integrated_pingyin_strs(query_str)
        suggest_size = config.get_value('/consts/global/algorithm/like_string_suggest_size')
        suggest_str_list = []
        for pingyin_str in pingyin_str_list:
            suggest_query_params = {'q': pingyin_str, 'suggest_size': suggest_size}
            suggest_dsl = qdsl_parser.get_suggest_qdl(None, suggest_index_cfg['type'], suggest_query_params)
            es_suggest_result = es_adapter.completion_suggest(suggest_dsl, suggest_index_cfg['host'],
                                                              suggest_index_cfg['index'])
            suggest_str_list.extend(self.__parse_completion_suggest_result(es_suggest_result))

        return map(lambda x: x[0], sorted(set(suggest_str_list), key=lambda x: x[1], reverse=True))

    def __get_fuzzy_query_result(self, origin_query_str, suggest_index_cfg):
        """
        对输入关键词执行fuzzy查询
        :param origin_query_str:
        :param suggest_index_cfg:
        :return:
        """
        fuzzy_dsl = {"query": {"fuzzy": {"name": origin_query_str}},
                     "size": config.get_value('/consts/global/algorithm/like_string_fuzzy_size')}
        es_fuzzy_query_result = es_adapter.query_docs(fuzzy_dsl, suggest_index_cfg['host'],
                                                      suggest_index_cfg['index'], suggest_index_cfg['type'])
        return self.__parse_fuzzy_query_result(es_fuzzy_query_result)

    def __parse_completion_suggest_result(self, es_suggest_result):
        """
        解析completion_suggest 查询结果
        :param es_suggest_result:
        :return:
        """
        if 'completion_suggest' not in es_suggest_result:
            return []
        return map(lambda completion_option: (completion_option['text'], completion_option['score']),
                   es_suggest_result['completion_suggest'][0]['options'])

    def __parse_fuzzy_query_result(self, es_fuzzy_query_result):
        """
        解析msearch查询结果，主要是suggest查询和fuzzy查询
        :param es_fuzzy_query_result:
        :return:
        """
        return map(lambda suggest_item: suggest_item['_source']['name'], es_fuzzy_query_result['hits']['hits'])

    def __merge_like_str_list(self, like_str_list, query_str, suggest_index_cfg):
        """
        整理相似字符串列表，主要操作有：删除掉和输入字符串相同的字符串；根据size截掉多余的相似字符串
        :param like_str_list:
        :param query_str:
        :param suggest_index_cfg:
        :return:
        """
        size = suggest_index_cfg.get('size') or config.get_value('/consts/global/query_size/like_str_size/default')
        if query_str in like_str_list:
            like_str_list.remove(query_str)
        result_list = []
        cur_size = 0
        for like_str in like_str_list:
            if like_str not in result_list:
                result_list.append(like_str)
                cur_size += 1
                if cur_size >= size:
                    break
        return result_list


like_str_algorithm = LikeQueryString()





