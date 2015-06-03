# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'
from elasticsearch import Elasticsearch
import re

from common.utils import *


es = None
insert_json_data = [{"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "1"}},
                    {"brandName": "半斤八两", "keywords": "1", "name": "手机001test"},
                    {"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "2"}},
                    {"brandName": "ww", "keywords": "", "mktPrice": 0, "name": "卖点1"},
                    {"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "3"}},
                    {"brandName": "苹果", "name": "iphone5s"},
                    {"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "4"}},
                    {"brandName": "dasdsadasdas", "keywords": "", "name": "gktest990"}]

update_json_data = [{"update": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "1"}},
                    {"doc": {"brandName": "半斤八两", "keywords": "update__1", "name": "手机001test"}},
                    {"update": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "20"}},
                    {"doc": {"brandName": "ww", "keywords": "", "mktPrice": 0, "name": "卖点1"}},
                    {"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "301"}},
                    {"brandName": "苹果_300", "name": "iphone5s_300"},
                    {"index": {"_index": "seach_test", "_type": "QmShopProduct", "_id": "4"}},
                    {"brandName": "dasdsadasdas", "keywords": "", "name": "gktest990"}]

update_json_data_1 = [
    {"update": {"_index": "search_platform-gonghuo-1.0.0", "_type": "Product", "_id": "1"}},
    {"doc": {"brandName": "半斤八两", "keywords": "update__1", "name": "手机001test"}}
]


def init():
    es = Elasticsearch('http://172.19.65.66:9200/')


def destroy():
    es = None


def test_bulk():
    es = Elasticsearch('http://172.19.65.66:9200/')
    insert_json_data[1]['keyworkds'] = '444444'
    insert_json_data[3]['keyworkds'] = '7777'
    result = es.bulk(insert_json_data)
    print result
    print "2 start ****************************************"
    result = es.bulk(insert_json_data)
    print result


def test_update():
    es = Elasticsearch('http://172.19.65.66:9200/')
    result = es.bulk(update_json_data)
    print result
    result = es.bulk([])
    print result


if __name__ == '__main__':
    regex = '(\"type\":\"add\"[\\d\\D]*\"sys\":2)|(\"sys\":2[\\d\\D]*\"type\":\"add\")'
    # regex = '\"sys\":2[\D\d]*'

    test_str = '{"type":"add","chainMasterId":"A892107","ids":"p5836:g22795:g22796:g22797:g22798;","operation":"stock","sys":2,}'
    print test_str.find(regex)
    print re.search(regex, test_str).group()

    test_str = '{"chainMasterId":"A892107","ids":"p5870:g23596;","operation":"stock","sys":2,"type":"update"}'
    fields = {}
    fields_temp = {
        "adminID": "chainMasterId\":\"(?P<adminID>[\\d\\D]+?)\"",
        "ids": "\"ids\":\"(?P<ids>[\\d\\D]+?)\""
    }
    for key in fields_temp:
        print unbind_variable(fields_temp[key], key, test_str)