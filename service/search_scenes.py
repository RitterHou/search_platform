# coding=utf-8
from collections import OrderedDict
from itertools import chain
import time

from common.adapter import es_adapter
from common.loggers import query_log


__author__ = 'liuzhaoming'


class SpuAndSkuDict(object):
    """
    SPU和SKU的存储结构，key为spuId，value[skuId,skuId1,skuId2]
    按照put的顺序存储
    """

    def __init__(self, **kwargs):
        self.__ordered_dict = OrderedDict()
        self.__ignore_spu_ids = []
        self.__ignore_sku = False
        if 'aggs_sku_size' in kwargs:
            self.__ignore_sku_size = kwargs['aggs_sku_size']
            self.__ignore_sku = True

    def put(self, spu_id, sku_id):
        """
        将spuId和skuId存放到数据结构中
        """
        if spu_id in self.__ordered_dict:
            spu_sku_ids = self.__ordered_dict[spu_id]
            if self.__ignore_sku:
                if spu_id in self.__ignore_spu_ids:
                    return
                elif len(spu_sku_ids) >= self.__ignore_sku_size:
                    self.__ignore_spu_ids.append(spu_id)
                    return
            spu_sku_ids.append(sku_id)
        else:
            self.__ordered_dict[spu_id] = [sku_id]

    def put_list(self, spu_id, sku_id_list):
        """
        将spuId和skuIdList存放到数据结构中
        """
        self.__ordered_dict[spu_id] = sku_id_list

    def get_spu_ids(self):
        return self.__ordered_dict.iterkeys()

    def get_sku_ids(self):
        return chain(*self.__ordered_dict.itervalues())

    def get_paged_dict(self, from_pos, page_size):
        """
        获取分页数据
        """
        cur_pos = 0
        put_size = 0
        start_put = False
        paged_dict = SpuAndSkuDict()
        for spu_id in self.__ordered_dict:
            if put_size >= page_size:
                break
            if start_put or cur_pos == from_pos:
                start_put = start_put or True
                paged_dict.put_list(spu_id, self.__ordered_dict[spu_id])
                put_size += 1
            cur_pos += 1
        return paged_dict

    def get_spu_size(self):
        return len(tuple(self.__ordered_dict.iterkeys()))


class SpuSearchBySku(object):
    """
    通过查询Sku聚合出对应的SPU，查询条件和聚合条件作用在SKU，分页信息作用在SPU
    结果以SPU的方式展示
    流程为：先查询所有符合条件的spuId、skuId，然后手工根据spuId和skuId进行分页，分页后在根据spuId和skuId分别查询数据
    如果原查询中带有aggs聚合，那么还要做一次聚合
    """


    def get_spu_by_sku(self, sku_dsl, es_cfg, args, parse_fields):
        from service.models import Aggregation

        start_time = time.time()
        spu_dsl = self.get_spu_sku_id_query_dsl(sku_dsl)
        es_scan_result = es_adapter.scan('1m', body=spu_dsl, preserve_order=True, **es_cfg)
        es_scan_result = tuple(es_scan_result)
        # 限制SPU中聚合的SKU数目
        aggs_sku_size = int(args['aggs_sku_size']) if 'aggs_sku_size' in args and int(args['aggs_sku_size']) > 0 else 0
        if aggs_sku_size > 0:
            spu_sku_dict = SpuAndSkuDict(aggs_sku_size=aggs_sku_size)
        else:
            spu_sku_dict = SpuAndSkuDict()
        for item in es_scan_result:
            if item['fields'].get('spuId') and item['fields'].get('skuId'):
                spu_id = item['fields'].get('spuId')[0]
                sku_id = item['fields'].get('skuId')[0]
                spu_sku_dict.put(spu_id, sku_id)
        total_size = spu_sku_dict.get_spu_size()
        page_spu_sku_dict = spu_sku_dict.get_paged_dict(sku_dsl.get('from') or 0, sku_dsl.get('size'))
        sku_id_list = list(page_spu_sku_dict.get_sku_ids())
        spu_id_list = list(page_spu_sku_dict.get_spu_ids())

        multi_search_body = [
            {'index': es_cfg['index'], 'type': es_adapter.get_spu_es_setting(parse_fields.get('adminId')).get('type')},
            {'query': {'ids': {'values': spu_id_list}}, 'size': sku_dsl.get('size')},
            {'index': es_cfg['index'], 'type': es_cfg['type']},
            self.generate_sku_query_dsl(sku_dsl, sku_id_list, es_cfg)]
        if 'aggs' in sku_dsl:
            # 如果原来的dsl带聚合，那么还需要额外做一次聚合操作
            sku_dsl['size'] = 0
            multi_search_body.extend(({'index': es_cfg['index'], 'type': es_cfg['type']}, sku_dsl))
        multi_search_results = es_adapter.multi_search(multi_search_body, es_cfg['host'], es_cfg['index'], None)
        spu_list = self.parse_spu_search_result(multi_search_results, page_spu_sku_dict,
                                                delete_goods_field=aggs_sku_size > 0)

        self.parse_sku_search_result(spu_list, args, multi_search_results)

        product_dict = {'root': spu_list, 'total': total_size}
        if 'aggs' in sku_dsl:
            aggs_search_response = multi_search_results['responses'][2]
            aggs_dict = Aggregation.objects.parse_es_result(aggs_search_response, args)
            query_log.info('get_spu_by_sku spends {0}', time.time() - start_time)
            return {'products': product_dict, 'aggregations': aggs_dict}, aggs_search_response
        query_log.info('get_spu_by_sku spends {0}', time.time() - start_time)
        return product_dict, None

    def generate_sku_query_dsl(self, sku_dsl, sku_id_list, es_cfg):
        """
        生成SKU查询DSL
        :param sku_dsl:
        :param sku_id_list:
        :param es_cfg:
        :return:
        """
        # sku_dsl = deepcopy(sku_dsl)
        sku_dsl = {'query': {'bool': {'must': []}}}
        if 'aggs' in sku_dsl:
            del sku_dsl['aggs']
        sku_dsl['from'] = 0
        sku_dsl['size'] = len(sku_id_list)
        sku_dsl['query']['bool']['must'].append({'ids': {'values': sku_id_list}})
        return sku_dsl

    def parse_sku_search_result(self, spu_list, args, multi_search_results):
        """
        解析SKU查询结果
        :param spu_list:
        :param args:
        :param multi_search_results:
        :return:
        """
        from service.models import Product

        sku_search_response = multi_search_results['responses'][1]
        sku_parse_result = Product.objects.parse_es_result(sku_search_response, args)
        for spu_item in spu_list:
            for sku_item in sku_parse_result['root']:
                if spu_item['spuId'] == sku_item['spuId']:
                    spu_item['skuList'].append(sku_item)

    def parse_spu_search_result(self, multi_search_results, page_spu_sku_dict, delete_goods_field=False):
        """
        解析SPU查询结果
        :param multi_search_results:
        :param page_spu_sku_dict:
        :return:
        """
        spu_search_response = multi_search_results['responses'][0]
        spu_list = []
        for spu_id in page_spu_sku_dict.get_spu_ids():
            for spu_item in spu_search_response['hits']['hits']:
                if spu_id == spu_item['_id']:
                    spu_item['_source']['skuList'] = []
                    if delete_goods_field:
                        del spu_item['_source']['goods']
                    spu_list.append(spu_item['_source'])
                    break
        return spu_list

    def get_spu_sku_id_query_dsl(self, sku_dsl):
        """
        获取SPUID SKUID fields 查询
        :param sku_dsl:
        :return:
        """
        spu_dsl = {'fields': ['skuId', 'spuId'], 'size': 1000}
        for key in sku_dsl:
            if key in ('aggs', 'from', 'size'):
                continue
            spu_dsl[key] = sku_dsl[key]
        return spu_dsl


spu_search_scene = SpuSearchBySku()




