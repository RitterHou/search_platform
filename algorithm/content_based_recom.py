# coding=utf-8
import math
import random

from common.configs import config
from common.utils import deep_merge


__author__ = 'liuzhaoming'


class ProductAttrVector(object):
    """
    商品属性向量，目前用品牌、价格、skuID、prop附加属性
    支持配置文件，可以在/consts/global/algorithm/content_based_recom/vectors指定
    """

    def __init__(self, product, range_values):
        self.vector_cfg = config.get_value('/consts/global/algorithm/content_based_recom/vectors')
        self.product = product
        self.range_values = range_values
        self.vector, self.weight = self.get_product_vector_and_weight()

    def get_product_vector_and_weight(self):
        """
        获取商品绝对的向量和权重
        """
        vector = {}
        weight = {}
        for (key, key_cfg) in self.vector_cfg.iteritems():
            if key_cfg.get('type') == 'range':
                max_value = self.range_values[key]['max']
                min_value = self.range_values[key]['min']
                vector[key] = float(self.product.get(key) - min_value) / (max_value - min_value)
                weight[key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
                    key].get('weight')
            elif key_cfg.get('type') == 'nest':
                if not isinstance(self.product.get(key), (list, tuple, set)):
                    continue
                prop_list = self.product.get(key)
                for prop_item in prop_list:
                    complete_key = '.'.join((key, prop_item['name']))
                    vector[complete_key] = 1
                    weight[complete_key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
                        key].get('weight')
            elif self.product.get(key):
                vector[key] = 1
                weight[key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
                    key].get('weight')
        return vector, weight

    def get_relative_product_vector(self, other_product):
        """
        获取商品的相对向量
        """
        other_vector = {}
        for key in self.vector:
            nest_point_index = key.find('.')
            if nest_point_index > -1:
                orgin_key = key[:nest_point_index]
                nest_key = key[nest_point_index + 1:]
                other_nest_obj_list = filter(lambda item: item.get('name') == nest_key, other_product.get(orgin_key))
                other_value = other_nest_obj_list[0].get('value') if other_nest_obj_list else None
                nest_obj_list = filter(lambda item: item.get('name') == nest_key, self.product.get(orgin_key))
                value = nest_obj_list[0].get('value') if nest_obj_list else None
                other_vector[key] = 1 if other_value == value else 0
            elif self.vector_cfg.get(key).get('type') == 'range':
                max_value = self.range_values[key]['max']
                min_value = self.range_values[key]['min']
                other_vector[key] = float(other_product.get(key) - min_value) / (max_value - min_value)
            else:
                other_vector[key] = 1 if other_product.get(key) == self.product.get(key) else 0
        return other_vector


    def calculate_cosine(self, other_product):
        """
        计算商品向量的余弦值
        """
        other_vector = self.get_relative_product_vector(other_product)
        numerator = sum(
            map(lambda key: self.vector[key] * other_vector[key] * math.pow(self.weight[key], 2), self.vector))
        denominator = math.sqrt(
            sum(map(lambda (key, value): math.pow(value * self.weight[key], 2),
                    self.vector.iteritems()))) or 0.00000000001
        other_denominator = math.sqrt(
            sum(map(lambda (key, value): math.pow(value * self.weight[key], 2),
                    other_vector.iteritems()))) or 0.00000000001
        return float(numerator) / (denominator * other_denominator)

    def __cal_weight(self, key):
        nest_point_index = key.find('.')
        if nest_point_index > -1:
            key = key[:nest_point_index]
        return self.vector_cfg.get(key).get('weight')


class ContentBasedRecommendation(object):
    """
    基于商品内容的推荐算法,可以采用多种算法，
    """

    def recommend_products_by_cosine(self, product_list, candidate_product_dict, recommend_cfg, range_dict):
        """
        采用"余弦相似性"推荐相似商品
        :param product_list:
        :param candidate_product_dict:
        :param recommend_cfg:
        :param range_dict:
        :return:
        """
        recommend_cfg = deep_merge(config.get_value('/consts/global/algorithm/content_based_recom/recommend'),
                                   recommend_cfg)
        total_cosine_similarities = []
        size = int(recommend_cfg['size'])
        for product in product_list:
            product_vector = ProductAttrVector(product, range_dict[product['type']])
            cosine_similarities = map(
                lambda cur_product: {"similarity": product_vector.calculate_cosine(cur_product),
                                     "product": cur_product},
                candidate_product_dict[product['type']])
            # 根据最小相似度过滤
            filter_cosine_similarities = filter(
                lambda item: item['similarity'] > float(recommend_cfg['min_cosine_similarity']),
                cosine_similarities)
            # 根据相似度排序
            filter_cosine_similarities.sort(lambda x, y: cmp(y['similarity'], x['similarity']))
            # 取出size*倍数个相似度
            filter_cosine_similarities = filter_cosine_similarities[
                                         : size * int(recommend_cfg['candidate_multiple'])]
            total_cosine_similarities.extend(filter_cosine_similarities)
        total_size = len(total_cosine_similarities)

        # 过滤掉商品自身
        total_cosine_similarities = filter(lambda item: item['similarity'] < 0.999999999999999,
                                           total_cosine_similarities)
        if total_size <= size:
            return map(lambda item: item['product'], total_cosine_similarities)
        else:
            random_product_list = random.sample(total_cosine_similarities, size)
            return_product_list = []
            cur_size = 0
            while cur_size < size:
                for random_product in random_product_list:
                    if random_product not in return_product_list:
                        return_product_list.append(random_product['product'])
                        cur_size += 1
                        if cur_size >= size:
                            return return_product_list
                random_product_list = random.sample(filter_cosine_similarities, size)
            return return_product_list


content_recom = ContentBasedRecommendation()
