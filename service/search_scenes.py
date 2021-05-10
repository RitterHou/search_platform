# coding=utf-8
import time
from collections import OrderedDict
from itertools import chain

from common.adapter import es_adapter, es7_adapter
from common.loggers import app_log

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

    def get_sku_ids_by_spu(self, spu_id):
        return self.__ordered_dict.get(spu_id)

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

    def get_spu_by_sku(self, sku_dsl, es_cfg, args, parse_fields, es_search_params=None):
        from service.models import Aggregation

        total_start_time = time.time()
        start_time = time.time()
        spu_dsl = self.get_spu_sku_id_query_dsl(sku_dsl)

        # elasticsearch-1.5.2和elasticsearch-7.5.2使用的是不同的scan查询接口
        if es_cfg.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            spu_dsl['_source'] = spu_dsl.pop('fields')
            es_scan_result = es7_adapter.scan('10s', body=spu_dsl, preserve_order=True,
                                              es_search_params=es_search_params, **es_cfg)
            # 这里的格式搞的这么复杂纯粹是为了向前兼容
            es_scan_result = map(lambda r: {'fields': {
                'spuId': [r['_source']['spuId']] if 'spuId' in r['_source'] else None,
                'skuId': [r['_source']['skuId']] if 'skuId' in r['_source'] else None,
            }}, es_scan_result['hits']['hits'])
        else:
            es_scan_result = es_adapter.scan('1m', body=spu_dsl, preserve_order=True, es_search_params=es_search_params,
                                             **es_cfg)
            es_scan_result = tuple(es_scan_result)

        # app_log.info("spu by sku scan id spends {0}  {1}", time.time()-start_time, parse_fields)
        start_time = time.time()
        build_start_time = time.time()
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
        # start_time = time.time()
        total_size = spu_sku_dict.get_spu_size()
        # app_log.info("spu by sku get spu size spends {0}  {1}", time.time() - start_time, parse_fields)
        # start_time = time.time()
        page_spu_sku_dict = spu_sku_dict.get_paged_dict(sku_dsl.get('from') or 0, sku_dsl.get('size'))
        # app_log.info("spu by sku get page spu spends {0}  {1}", time.time() - start_time, parse_fields)
        # start_time = time.time()
        sku_id_list = list(page_spu_sku_dict.get_sku_ids())
        # app_log.info("spu by sku get sku id list spends {0}  {1}", time.time() - start_time, parse_fields)
        # start_time = time.time()
        spu_id_list = list(page_spu_sku_dict.get_spu_ids())
        # app_log.info("spu by sku get spu id list spends {0}  {1}", time.time() - start_time, parse_fields)
        # app_log.info("spu by sku build ids spends {0}  {1}", time.time() - build_start_time, parse_fields)
        start_time = time.time()

        if 'spu_index' in es_cfg:
            spu_index = es_cfg['spu_index']
            spu_index, _, _ = es_adapter.get_es_doc_keys({'index': spu_index}, kwargs=parse_fields)
        else:
            spu_index = es_cfg['index']

        admin_id = parse_fields['adminId']
        # 针对elasticsearch的不同版本，选择使用不同的数据查询逻辑
        if es_cfg.get('destination_type', 'elasticsearch') == 'elasticsearch7':
            # elasticsearch-7.5.2的查询
            spu_terms = []
            for value in spu_id_list:
                spu_terms.append({"term": {'spuId': value}})

            sku_terms = []
            for value in sku_id_list:
                sku_terms.append({"term": {'skuId': value}})

            multi_search_body = [
                {'index': spu_index},
                {'query': {'bool': {'must': [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": spu_terms
                        }
                    },
                    {
                        'term': {
                            '_adminId': admin_id
                        }
                    }
                ]}}, 'size': sku_dsl.get('size')},
                {'index': es_cfg['index']},
                {'query': {'bool': {'must': [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": sku_terms
                        }
                    },
                    {
                        'term': {
                            '_adminId': admin_id
                        }
                    }
                ]}}, 'size': len(sku_id_list)}
            ]
            if 'aggs' in sku_dsl:
                # 如果原来的dsl带聚合，那么还需要额外做一次聚合操作
                sku_dsl['size'] = 0
                multi_search_body.extend(({'index': es_cfg['index']}, sku_dsl))
            multi_search_results = es7_adapter.multi_search(multi_search_body, es_cfg['host'], es_cfg['index'], None)
            spu_list = self.parse_spu_search_result(multi_search_results, page_spu_sku_dict,
                                                    delete_goods_field=aggs_sku_size > 0)

            self.parse_sku_search_result(spu_list, args, multi_search_results, page_spu_sku_dict)

            product_dict = {'root': spu_list, 'total': total_size}
            if 'aggs' in sku_dsl:
                aggs_search_response = multi_search_results['responses'][2]
                aggs_dict = Aggregation.objects.parse_es_result(aggs_search_response, args)
                return {'products': product_dict, 'aggregations': aggs_dict}, aggs_search_response
            return product_dict, None
        else:
            # elasticsearch-1.5.2的查询
            if 'spu_type' in es_cfg:
                spu_type = es_cfg['spu_type']
            else:
                spu_type = es_adapter.get_spu_es_setting(parse_fields.get('adminId')).get('type')

            multi_search_body = [
                {'index': spu_index, 'type': spu_type},
                {'query': {'bool': {'must': [
                    {
                        'terms': {
                            'spuId': spu_id_list,
                            'minimum_should_match': 1
                        }
                    },
                    {
                        'term': {
                            '_adminId': admin_id
                        }
                    }
                ]}}, 'size': sku_dsl.get('size')},
                {'index': es_cfg['index'], 'type': es_cfg['type']},
                self.generate_sku_query_dsl(sku_dsl, sku_id_list, es_cfg, admin_id)]
            if 'aggs' in sku_dsl:
                # 如果原来的dsl带聚合，那么还需要额外做一次聚合操作
                sku_dsl['size'] = 0
                multi_search_body.extend(({'index': es_cfg['index'], 'type': es_cfg['type']}, sku_dsl))
            multi_search_results = es_adapter.multi_search(multi_search_body, es_cfg['host'], es_cfg['index'], None)
            # app_log.info("spu by sku multi search spends {0}  {1}", time.time() - start_time, parse_fields)
            spu_list = self.parse_spu_search_result(multi_search_results, page_spu_sku_dict,
                                                    delete_goods_field=aggs_sku_size > 0)

            self.parse_sku_search_result(spu_list, args, multi_search_results, page_spu_sku_dict)

            product_dict = {'root': spu_list, 'total': total_size}
            if 'aggs' in sku_dsl:
                aggs_search_response = multi_search_results['responses'][2]
                aggs_dict = Aggregation.objects.parse_es_result(aggs_search_response, args)
                # app_log.info('spu by sku total spends {0}  {1}', time.time() - total_start_time, parse_fields)
                return {'products': product_dict, 'aggregations': aggs_dict}, aggs_search_response
            # app_log.info('spu by sku total spends {0}  {1}', time.time() - total_start_time, parse_fields)
            return product_dict, None

    def generate_sku_query_dsl(self, sku_dsl, sku_id_list, es_cfg, admin_id):
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
        sku_dsl['query']['bool']['must'].append({
            "terms": {
                "skuId": sku_id_list,
                "minimum_should_match": 1
            }
        })
        sku_dsl['query']['bool']['must'].append({
            'term': {
                '_adminId': admin_id
            }
        })
        return sku_dsl

    def parse_sku_search_result(self, spu_list, args, multi_search_results, page_spu_sku_dict):
        """
        解析SKU查询结果
        :param spu_list:
        :param args:
        :param multi_search_results:
        :param page_spu_sku_dict
        :return:
        """
        from service.models import Product

        sku_search_response = multi_search_results['responses'][1]
        sku_parse_result = Product.objects.parse_es_result(sku_search_response, args)
        sku_dict = {}
        for sku_item in sku_parse_result['root']:
            sku_dict[sku_item['skuId']] = sku_item
        for spu_item in spu_list:
            sku_ids = page_spu_sku_dict.get_sku_ids_by_spu(spu_item['spuId'])
            if not sku_ids:
                continue
            for sku_id in sku_ids:
                sku_item = sku_dict.get(sku_id)
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
                if spu_id == spu_item['_source']['spuId']:
                    spu_item['_source']['skuList'] = []
                    if delete_goods_field and 'goods' in spu_item['_source']:
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
        spu_dsl = {'fields': ['skuId', 'spuId'], 'size': 10000}
        for key in sku_dsl:
            if key in ('aggs', 'from', 'size'):
                continue
            spu_dsl[key] = sku_dsl[key]
        return spu_dsl


spu_search_scene = SpuSearchBySku()
