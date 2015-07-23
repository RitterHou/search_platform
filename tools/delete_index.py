# coding = utf-8
from elasticsearch import Elasticsearch

__author__ = 'liuzhaoming'


class DeleteData(object):
    def __init__(self, host='http://192.168.65.135:9200'):
        self.es = Elasticsearch(host)

    def delete_by_query(self, index, doc_type, dsl):
        data = self.es.delete_by_query(index, doc_type, body=dsl)
        print data


if __name__ == '__main__':
    operator = DeleteData()
    operator.delete_by_query('qmshop-gonghuo-1.0.0', 'Product', dsl={'query': {'term': {'sys': 2}}})
