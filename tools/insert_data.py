# coding=utf-8
import json

from elasticsearch import Elasticsearch

from common.configs import config


__author__ = 'liuzhaoming'


def insert_data():
    es = Elasticsearch('http://172.19.65.66:9200')
    f = open('./qmshop-a910401-1.0.0.json')
    data_list = json.load(f)
    es.indices.put_mapping(index='qmshop-a910401-1.0.0', doc_type='Product',
                           body=config.get_value('es_index_setting/product/mapping'))
    # es.indices.put_mapping(index='qmshop-a910401-1.0.0', doc_type='Product')

    doc_list = []
    for data in data_list:
        doc_list.append({'index': {"_id": data['skuId']}})
        doc_list.append(data)
    es_result = es.bulk(index='qmshop-a910401-1.0.0', doc_type='Product', body=doc_list)
    print es_result


if __name__ == '__main__':
    insert_data()
