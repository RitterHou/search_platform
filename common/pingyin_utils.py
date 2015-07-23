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
        result = []
        if not input_str:
            return result
        result.extend(self.combine_element(pinyin(input_str, style=pypinyin.NORMAL, heteronym=True), 0))
        first_letter_list = pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True)
        initials_letter_list = pinyin(input_str, style=pypinyin.INITIALS, heteronym=True)
        merge_letter_list = map(lambda a_list, b_list: filter(lambda item: item, set(a_list + b_list)),
                                first_letter_list, initials_letter_list)
        result.extend(self.combine_element(merge_letter_list, 0))
        # result.extend(self.combine_element(pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True), 0))
        # result.extend(self.combine_element(pinyin(input_str, style=pypinyin.INITIALS, heteronym=True), 0))
        result.extend(self.combine_low_up_chars(input_str))
        # result + [ele.replace(self.separator) for ele in result]
        result.extend(list(map((lambda ele: ele.replace(separator, '')), result)))
        return list(set(result))

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
        if start == len(input_list) - 1:
            return input_list[start]
        return [separator.join(ele) for ele in
                product(input_list[start], self.combine_element(input_list, start + 1, separator))]

    def combine_low_up_chars(self, word):
        result = []
        if re.search(self.english_filter_regr, word):
            result.append(word.lower())
            result.append(word.upper())
        return result


pingyin_utils = PinyinTools()

if __name__ == '__main__':
    # print pingyin_utils.get_pingyin_combination(u'中')
    # print pingyin_utils.get_pingyin_combination(u'中重')
    print pingyin_utils.get_pingyin_combination(u'安踏凉鞋测试')
    input_str = u'安踏凉鞋测试'
    first_letter_list = pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True)
    initials_letter_list = pinyin(input_str, style=pypinyin.INITIALS, heteronym=True)
    merge_letter_list = imap(lambda a_list, b_list: filter(lambda item: item, set(a_list + b_list)), first_letter_list,
                             initials_letter_list)
    print list(merge_letter_list)
    print pingyin_utils.get_integrated_pingyin_strs(u'安踏凉鞋测试')
    print pingyin_utils.get_integrated_pingyin_strs(u'重庆火锅')