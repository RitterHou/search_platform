# coding=utf-8
from elasticsearch import Elasticsearch

__author__ = 'liuzhaoming'


def get_search_platform_indexs(es):
    """
    获取所有搜索引擎建立的索引
    :param es:
    :return:
    """
    return filter(lambda index_name: index_name.startswith('qmshop-') or index_name.startswith('suggest-'),
                  es.indices.get_settings().iterkeys())


def create_snmpshot(es, index_name_list, repository='log_backup', snmapshot_name='snapshot_search_platform_1'):
    """
    创建snmpshot
    :param es:
    :param index_name_list:
    :return:
    """
    return es.snapshot.create(repository, snmapshot_name, body={'indices': ','.join(index_name_list)})


if __name__ == '__main__':
    es = Elasticsearch('http://192.168.65.133:9200')
    index_name_list = get_search_platform_indexs(es)
    print index_name_list
    result = create_snmpshot(es, index_name_list)
    print 'create snmpshot finish {0}'.format(result)



