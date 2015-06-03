# -*- coding: utf-8 -*-
# Create your models here.
__author__ = 'liuzhaoming'

from common.connections import EsConnectionFactory
from common.loggers import debug_log, query_log as app_log
from qdsl_parseres import qdsl_parser


class EsModel(object):
    field_config = {}

    def __init__(self, **args):
        self.__dict__.update([(key, value) for (key, value) in args.iteritems() if key in self.field_config])


class EsProductManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsProductManger.get::')
    def get(self, es_config, index_name, doc_type, args):
        """
        通过ES查询产品数据
        :param index_name:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        if not es_connection:
            return {}

        qdsl = qdsl_parser.get_product_query_qdsl(index_name, doc_type, args, es_connection)
        app_log.info('Get product qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args, qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result)
        app_log.info('EsProductManager get return size is {0}', result['total'] if 'total' in result else 'omitted')
        return result

    def bulk_save(self, body):
        """
        批量保存ES数据
        :param body:
        :return:
        """
        pass
        # es_connection = self.connection_pool.get_es_connection(index_name)

    def parse_es_result(self, es_result):
        """
        解析ES查询结果
        :param es_result:
        :return:
        """
        if 'hits' in es_result and es_result['hits'] and 'hits' in es_result['hits']:
            total = es_result['hits']['total']
            doc_list = es_result['hits']['hits']
            product_list = map(lambda doc: doc['_source'], doc_list)
        elif '_source' in es_result:
            total = 1
            product_list = [es_result['_source']]
        else:
            total = 0
            product_list = []

        return {'root': product_list, 'total': total}


class EsAggManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsAggManager.get::')
    def get(self, es_config, index_name, doc_type, args):
        """
        查询聚合数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_agg_qdl(index_name, doc_type, args, es_connection)
        debug_log.print_log('get agg qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args, qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result)
        app_log.info('EsAggManager get return is {0}', result)
        return result

    def parse_es_result(self, es_result):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        result = {}
        if 'aggregations' not in es_result:
            return result

        agg_result = es_result['aggregations']
        is_last_cat = self.__is_last_cat(agg_result)
        if 'brand' in agg_result:
            result['brand'] = self.__parse_nomal_agg_result(agg_result, 'brand')
        if 'cats' in agg_result:
            result['cats'] = self.__parse_cats_agg_result(agg_result, 'cats', is_last_cat)
        if 'props' in agg_result:
            result['props'] = self.__parse_prop_agg_result(agg_result, 'props', is_last_cat)

        return result

    def __parse_nomal_agg_result(self, agg_result_dict, field):
        """
        解析普通的字段聚合结果
        :param agg_result_dict:
        :param field:
        :return:
        """
        return agg_result_dict[field]['buckets'] if field in agg_result_dict else []

    def __parse_prop_agg_result(self, agg_result_dict, field, is_last_cat=False):
        """
        解析扩展属性聚合结果
        :param agg_result_dict:
        :param field:
        :param is_last_cat:
        :return:
        """
        if field not in agg_result_dict or not is_last_cat:
            return []

        prop_field_list = agg_result_dict[field]['name']['buckets']
        return map(
            lambda item: {'key': item['key'], 'doc_count': item['doc_count'], 'childs': item['value']['buckets']},
            prop_field_list)

    def __parse_cats_agg_result(self, agg_result_dict, field, is_last_cart):
        """
        解析路径聚合结果，可能会聚合很多层
        :param agg_result_dict:
        :param field:
        :param is_last_cart:
        :return:
        """
        if field not in agg_result_dict:
            return []

        prop_field_list = agg_result_dict[field]['name']['buckets']
        return map(lambda item: self.__get_cats_agg_result_item(item), prop_field_list)

    def __get_cats_agg_result_item(self, result_item):
        if 'childs' not in result_item:
            return result_item
        value_list = map(lambda item: self.__get_cats_agg_result_item(item), result_item['childs']['name']['buckets'])
        return {'key': result_item['key'], 'doc_count': result_item['doc_count'], 'childs': value_list}

    def __is_last_cat(self, agg_result_dict):
        """
        判断是否是最后一层，如果是最后一层需要返回prop属性聚合信息
        :param agg_result_dict:
        :param field:
        :return:
        """
        cats_agg_result = agg_result_dict['cats']['name']['buckets']
        wheel_cats_agg_result = agg_result_dict['wheel_cats']['name']['buckets']
        return self.__get_cats_level(cats_agg_result) == self.__get_cats_level(wheel_cats_agg_result)

    def __get_cats_level(self, cats_agg_list):
        """
        获取路径最大层次
        :param cats_agg_list:
        :return:
        """
        if len(cats_agg_list) == 0 or 'childs' not in cats_agg_list[0]:
            return 0
        return 1 + self.__get_cats_level(cats_agg_list[0]['childs']['name']['buckets'])


class EsSuggestManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsSuggestManger.get::')
    def get(self, es_config, index_name, doc_type, args):
        """
        查询Suggest数据
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_suggest_qdl(index_name, doc_type, args)
        debug_log.print_log('get suggest qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                            qdsl)
        es_result = es_connection.suggest(index=index_name, body=qdsl)

        result = self.parse_es_result(es_result)
        app_log.info('EsSuggestManager get return is {0}', result)
        return result

    def parse_es_result(self, es_result):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        if 'completion_suggest' not in es_result or not es_result['completion_suggest'] or 'options' not in \
                es_result['completion_suggest'][0]:
            return []
        options = es_result['completion_suggest'][0]['options']
        return map(lambda suggest_res: {'key': suggest_res['text'], 'doc_count': suggest_res['payload']['hits']},
                   options)


class EsSearchManager(object):
    connection_pool = EsConnectionFactory

    @debug_log.debug('EsSearchManager.get::')
    def get(self, es_config, index_name, doc_type, args):
        """
        查询聚合数据
        :param es_config:
        :param index_name:
        :param doc_type:
        :param args:
        :return:
        """
        es_connection = self.connection_pool.get_es_connection(es_config=es_config, create_index=False)
        qdsl = qdsl_parser.get_search_qdl(index_name, doc_type, args, es_connection)
        debug_log.print_log('get search qdsl index={0} , type={1} , args={2}, qdsl={3}', index_name, doc_type, args,
                            qdsl)
        es_result = es_connection.search(index_name, doc_type if doc_type != 'None' else None, body=qdsl)
        result = self.parse_es_result(es_result)
        app_log.info('EsSearchManager get return is omitted')
        return result

    def parse_es_result(self, es_result):
        """
        解析ES返回结果
        :param es_result:
        :return:
        """
        product_result = Product.objects.parse_es_result(es_result)
        agg_result = Aggregation.objects.parse_es_result(es_result)
        search_result = {}
        if product_result and product_result.get('root'):
            search_result['products'] = product_result
        if agg_result:
            search_result['aggregations'] = agg_result
        return search_result


class Product(EsModel):
    objects = EsProductManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Suggest(EsModel):
    objects = EsSuggestManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Aggregation(EsModel):
    objects = EsAggManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


class Search(EsModel):
    objects = EsSearchManager()

    def __init__(self, **args):
        EsModel.__init__(**args)


if __name__ == '__main__':
    from elasticsearch import Elasticsearch

    es = Elasticsearch('http://172.19.65.66:9200')
    bulk_body = es._bulk_body([{'1': 22, '2': 'value2'}, {
        "name": "咖啡特浓即饮罐装24*180ML",
        "url": "http://www.esunny.com/bProductListDetailPreview.do?pid=2397&categoryid=312&brandid=106",
        "image": "http://www.esunny.com/photo/1393574402654.jpg",
        "barcode": "6917878028606",
        "relative_shops": [
            {
                "shop": "宇商网",
                "url": "http://www.esunny.com/bProductListDetailPreview.do?pid=2397&categoryid=312&brandid=106",
                "price": "￥"
            }
        ],
        "details": [
            {
                "商品规格": "24*180ML"
            }
        ],
        "id": "6917878028606"
    }])
    print bulk_body

