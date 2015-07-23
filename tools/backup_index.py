# coding = utf-8
import json

from elasticsearch import Elasticsearch


__author__ = 'liuzhaoming'


class Backup(object):
    def __init__(self, host='http://172.19.65.66:9200'):
        self.es = Elasticsearch(host)

    def back(self, index, doc_type, dsl={'query': {'match_all': {}}, 'from': 0, 'size': 20000}, file_name=None):
        es_result = self.es.search(body=dsl, index=index, doc_type=doc_type)
        doc_list = es_result['hits']['hits']
        doc_list = map(lambda doc: doc['_source'], doc_list)
        f = open(file_name if file_name else './' + index + '.json', 'w')
        json.dump(doc_list, f)
        f.close()
        return doc_list


if __name__ == '__main__':
    backup = Backup()
    backup.back('qmshop-a910401-1.0.0', 'Product')
    # backup.back('qmshop-gonghuo-1.0.0', 'Product', dsl={'query': {'term': {'sys': 2}}, 'from': 0, 'size': 20000},
    #             file_name='gonghuo_sys_2.json')
