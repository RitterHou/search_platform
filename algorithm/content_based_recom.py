# coding=utf-8
import math
import random

from common.configs import config
from common.utils import deep_merge, get_cats_path


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
        self.text_cosine_vector = {}
        self.vector, self.weight = self.get_product_vector_and_weight()

    def get_product_vector_and_weight(self):
        """
        获取商品绝对的向量和权重
        """
        vector = {}
        weight = {}
        for (key, key_cfg) in self.vector_cfg.iteritems():
            if key_cfg.get('type') == 'range':
                self.__calculate_range_vector(key, vector, weight)
            elif key_cfg.get('type') == 'nest':
                if not isinstance(self.product.get(key), (list, tuple, set)):
                    continue
                self.__calculate_nest_vector(key, vector, weight)
            elif key_cfg.get('type') == 'cats':
                if not isinstance(self.product.get(key), list):
                    continue
                self.__calculate_cat_vector(key, vector, weight)
            elif key_cfg.get('type') == 'cosine':
                self.text_cosine_vector[key] = TextCosineVector(self.product.get(key))
                vector[key] = 1
                weight[key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
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
        cat_vectors = None
        for key in self.vector:
            nest_point_index = key.find('.')
            if nest_point_index > -1:
                origin_key = key[:nest_point_index]
                nest_key = key[nest_point_index + 1:]
                other_nest_obj_list = filter(lambda item: item.get('name') == nest_key, other_product.get(origin_key))
                other_value = other_nest_obj_list[0].get('value') if other_nest_obj_list else None
                nest_obj_list = filter(lambda item: item.get('name') == nest_key, self.product.get(origin_key))
                value = nest_obj_list[0].get('value') if nest_obj_list else None
                other_vector[key] = 1 if other_value == value else 0
            elif self.vector_cfg.get(key) is None:
                # cats的聚合key
                if cat_vectors is None:
                    cat_vectors = self._get_cats_vector(other_product)
                other_vector[key] = 1 if key in cat_vectors else 0
            elif self.vector_cfg.get(key).get('type') == 'range':
                max_value = self.range_values[key]['max']
                min_value = self.range_values[key]['min']
                division = max_value - min_value
                other_vector[key] = float(other_product.get(key) if other_product.get(
                    key) else 0 - min_value) / division if division != 0 else 0
            elif self.vector_cfg.get(key).get('type') == 'cosine':
                other_vector[key] = self.text_cosine_vector[key].get_cosine_similarity(other_product.get(key))
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

    def _get_cats_relative_vector(self, product, key='cats'):
        """
        获取cats属性的相对向量值
        """
        vectors = []
        if not isinstance(product.get(key), list):
            return vectors
        cat_list = product.get(key)
        for cat_item in cat_list:
            cat_buffer = []
            while cat_item and isinstance(cat_item, dict) and 'childs' in cat_item:
                if cat_item.get('name'):
                    cat_buffer.append(cat_item.get('name'))
                cat_item = cat_item['childs'][0] if cat_item['childs'] else None
            if cat_buffer:
                complete_key = '-'.join(cat_buffer)
                vectors.append(complete_key)
        return vectors

    def __calculate_cat_vector(self, key, vector, weight):
        """
        计算类目的向量表示
        """
        cat_list = self.product.get(key)
        for cat_item in cat_list:
            cat_buffer = []
            while cat_item and isinstance(cat_item, dict) and 'childs' in cat_item:
                if cat_item.get('name'):
                    cat_buffer.append(cat_item.get('name'))
                cat_item = cat_item['childs'][0] if cat_item['childs'] else None
            if cat_buffer:
                complete_key = '-'.join(cat_buffer)
                vector[complete_key] = 1
                weight[complete_key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
                    key].get('weight')

    def __calculate_range_vector(self, key, vector, weight):
        """
        计算数值范围的向量
        """
        max_value = self.range_values[key]['max']
        min_value = self.range_values[key]['min']
        division = max_value - min_value
        vector[key] = float(self.product.get(key) if self.product.get(
            key) else 0 - min_value) / division if division != 0 else 0
        weight[key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
            key].get('weight')
    def __calculate_nest_vector(self, key, vector, weight):
        """
        计算嵌套属性的向量
        """
        prop_list = self.product.get(key)
        for prop_item in prop_list:
            complete_key = '.'.join((key, prop_item['name']))
            vector[complete_key] = 1
            weight[complete_key] = 1 if self.vector_cfg[key].get('weight') is None else self.vector_cfg[
                key].get('weight')
class TextCosineVector(object):
    """
    文本余弦相似度
    """
    def __init__(self, text):
        self.statics = self.get_chinese_char_statics(text)
    def get_chinese_char_statics(self, text):
        """
        获取文本的汉字统计值
        """
        statics = {}
        if not text:
            return statics
        for char in text:
            a = ord(char)
            if 0x4DFF < a < 0x9FA6:
                if a in statics:
                    statics[a] += 1
                else:
                    statics[a] = 1
        return statics
    def get_cosine_similarity(self, text):
        """
        获取和文本的余弦相似度
        """
        other_statics = self.get_chinese_char_statics(text)
        for char, num in self.statics.iteritems():
            if char in other_statics:
                other_statics[char] = [other_statics[char], num]
            else:
                other_statics[char] = [0, num]
        if not len(other_statics):
            return 0
        mul_sum = 0
        square_sum1 = 0
        square_sum2 = 0
        for char, nums in other_statics.iteritems():
            if isinstance(nums, list):
                num = nums[1]
                other_num = nums[0]
            else:
                num = 0
                other_num = nums
            mul_sum += num * other_num
            square_sum1 += num * num
            square_sum2 += other_num * other_num
        if square_sum1 == 0 or square_sum2 == 0:
            return 0
        return mul_sum / math.sqrt(square_sum1 * square_sum2)
class ContentBasedRecommendation(object):
    """
    基于商品内容的推荐算法,可以采用多种算法，
    """

    def recommend_products_by_cosine(self, product_list, candidate_product_dict, recommend_cfg, range_dict, tag):
        """
        采用"余弦相似性"推荐相似商品
        :param product_list:
        :param candidate_product_dict:
        :param recommend_cfg:
        :param range_dict:
        :param tag 类目的根标签，b2c 或者 b2b
        :return:
        """
        recommend_cfg = deep_merge(config.get_value('/consts/global/algorithm/content_based_recom/recommend'),
                                   recommend_cfg)
        total_cosine_similarities = []
        size = int(recommend_cfg['size'])
        for product in product_list:
            cat_path_key = get_cats_path(product, tag)
            product_vector = ProductAttrVector(product, range_dict[cat_path_key])
            cosine_similarities = map(
                lambda cur_product: {"similarity": product_vector.calculate_cosine(cur_product),
                                     "product": cur_product},
                candidate_product_dict[cat_path_key])
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
