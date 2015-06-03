# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

from itertools import product

import re
from pypinyin import pinyin
import pypinyin


class PinyinTools(object):
    separator = ' '
    english_filter_regr = u'[A-Za-z0-9]+'

    def get_pingyin_combination(self, input_str):
        result = []
        if not input_str:
            return result
        result.extend(self.combine_element(pinyin(input_str, style=pypinyin.NORMAL, heteronym=True), 0))
        result.extend(self.combine_element(pinyin(input_str, style=pypinyin.FIRST_LETTER, heteronym=True), 0))
        result.extend(self.combine_element(pinyin(input_str, style=pypinyin.INITIALS, heteronym=True), 0))
        result.extend(self.combine_low_up_chars(input_str))
        # result + [ele.replace(self.separator) for ele in result]
        result.extend(list(map((lambda ele: ele.replace(self.separator, '')), result)))
        return list(set(result))

    def combine_element(self, input_list, start):
        if start == len(input_list) - 1:
            return input_list[start]
        return [self.separator.join(ele) for ele in
                product(input_list[start], self.combine_element(input_list, start + 1))]

    def combine_low_up_chars(self, word):
        result = []
        if re.search(self.english_filter_regr, word):
            result.append(word.lower())
            result.append(word.upper())
        return result


pingyin_utils = PinyinTools()

if __name__ == '__main__':
    print pingyin_utils.get_pingyin_combination(u'中')
    print pingyin_utils.get_pingyin_combination(u'中重')