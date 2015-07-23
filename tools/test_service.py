# coding=utf-8
def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()

from itertools import chain

from elasticsearch import Elasticsearch
from re import search

from common.configs import config


def _parse_index_mapping(index_name, mapping):
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


def migrate_es_index(es_connection, before_index, before_type, after_index=None, after_type=None, is_delete=True):
    """
    ES index数据迁移
    :param host:
    :param before_index:
    :param before_type:
    :param after_index:
    :param after_type:
    :param is_delete:
    :return:
    """
    es = es_connection
    es_result = es.search(body={'query': {'match_all': {}}, 'from': 0, 'size': 20000}, index=before_index,
                          doc_type=before_type)
    doc_list = es_result['hits']['hits']
    doc_list = map(lambda doc: doc['_source'], doc_list)
    # es.indices.delete(before_index)
    # es.indices.create(before_index)
    es.indices.delete_mapping(before_index, before_type)
    es.indices.put_mapping(index=after_index or before_index, doc_type=after_type or before_type,
                           body=config.get_value('es_index_setting/suggest/mapping'))
    for data in doc_list:
        es.index(index=after_index or before_index, doc_type=after_type or before_type, body=data, id=data['skuId'])


def filter_invalid_suggest_mapping(index_type_dict_list, es_connection):
    matched_index_type_dict_list = []
    for index_type_dict in index_type_dict_list:
        if not search(r'suggest-a[\d\D]+?-1.0.0', index_type_dict['index']):
            continue
        mapping_dict = es_connection.indices.get_mapping(index_type_dict['index'], index_type_dict['type'])
        mapping = mapping_dict[index_type_dict['index']]['mappings'][index_type_dict['type']]['properties']
        if 'suggest' in mapping and 'type' in mapping['suggest'] and mapping['suggest']['type'] == 'completion':
            continue

        matched_index_type_dict_list.append(index_type_dict)
    return matched_index_type_dict_list


def filter_invalid_datetime_mapping(index_type_dict_list, es_connection):
    matched_index_type_dict_list = []
    for index_type_dict in index_type_dict_list:
        if not search(r'qmshop-a[\d\D]+?-1.0.0', index_type_dict['index']) or not search(r'Product',
                                                                                         index_type_dict['type']):
            continue
        mapping_dict = es_connection.indices.get_mapping(index_type_dict['index'], index_type_dict['type'])
        mapping = mapping_dict[index_type_dict['index']]['mappings'][index_type_dict['type']]['properties']
        if 'updateTime' not in mapping:
            continue
        matched_index_type_dict_list.append(index_type_dict)
    return matched_index_type_dict_list


if __name__ == '__main__':
    host = 'http://192.168.65.131:9200'
    # host = 'http://172.19.65.66:9200'
    es_connection = Elasticsearch(hosts=host)
    mapping_dict = es_connection.indices.get_mapping()
    index_type_dict_list = list(chain(
        *[_parse_index_mapping(index_name, mapping_dict[index_name]) for index_name in mapping_dict]))
    matched_index_type_dict_list = filter(
        lambda index_type_dict: search(r'suggest-[\d\D]+?-1.0.0', index_type_dict['index']), index_type_dict_list)
    matched_index_type_dict_list = filter_invalid_suggest_mapping(matched_index_type_dict_list, es_connection)
    # matched_index_type_dict_list = filter_invalid_datetime_mapping(index_type_dict_list, es_connection)
    for index_type_dict in matched_index_type_dict_list:
        migrate_es_index(es_connection, index_type_dict['index'], index_type_dict['type'])