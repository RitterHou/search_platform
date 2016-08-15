# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

from itertools import product, imap

import re
from pypinyin import pinyin
import pypinyin

SEPARATOR = ' '


class PinyinTools(object):
    english_filter_regr = u'[A-Za-z0-9]+'

    def get_pingyin_combination(self, input_str, separator=SEPARATOR):
        """
        获取词汇的拼音综合，多音字会有多个读音组合
        水果 的返回结果为：["shui guo","shuiguo","s g","shg","sh g","sg"]
        :param input_str:
        :param separator:
        :return:
        """
        if not input_str:
            return []
        pinyin_list = self.combine_element(pinyin(input_str, style=pypinyin.NORMAL, heteronym=True), 0)
        first_letter_list = pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True)
        initials_letter_list = pinyin(input_str, style=pypinyin.INITIALS, heteronym=True)
        merge_letter_list = map(lambda a_list, b_list: filter(lambda item: item, set(a_list + b_list)),
                                first_letter_list, initials_letter_list)
        pinyin_list += self.combine_element(merge_letter_list, 0) + self.combine_low_up_chars(input_str)
        # result.extend(self.combine_element(pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True), 0))
        pinyin_list += map((lambda element: element.replace(separator, '')), pinyin_list)

        # result + [ele.replace(self.separator) for ele in result]
        return list(set(pinyin_list))

    def get_integrated_pingyin_strs(self, input_str):
        """
        获取词汇的完整拼音，包含多音词
        :param input_str:
        :return:
        """
        if not input_str:
            return []
        return self.combine_element(pinyin(input_str, style=pypinyin.NORMAL, heteronym=True), 0, '')


    def combine_element(self, input_list, start, separator=SEPARATOR):
        """
        组合各个字的拼音
        :param input_list:
        :param start:
        :param separator:
        :return:
        """
        if start == len(input_list) - 1:
            return input_list[start]
        return [separator.join(element) for element in
                product(input_list[start], self.combine_element(input_list, start + 1, separator))]

    def combine_low_up_chars(self, word):
        """
        组合大小写字符
        :param word:
        :return:
        """
        if re.search(self.english_filter_regr, word):
            return [word.lower(), word.upper()]

        return []


pingyin_utils = PinyinTools()

if __name__ == '__main__':
    # print pingyin_utils.get_pingyin_combination(u'中')
    # print pingyin_utils.get_pingyin_combination(u'中重')
    print pingyin_utils.get_pingyin_combination(u'安踏凉鞋测试')
    input_str = u'安踏凉鞋测试'
    _first_letter_list = pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True)
    _initials_letter_list = pinyin(input_str, style=pypinyin.INITIALS, heteronym=True)
    _merge_letter_list = imap(lambda a_list, b_list: filter(lambda item: item, set(a_list + b_list)), _first_letter_list,
                             _initials_letter_list)
    print list(_merge_letter_list)
    print pingyin_utils.get_integrated_pingyin_strs(u'安踏凉鞋测试')
    print pingyin_utils.get_integrated_pingyin_strs(u'重庆火锅')