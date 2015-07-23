# coding=utf-8
from common.configs import config

__author__ = 'liuzhaoming'


class EqualSectionPartitions(object):
    """
    根据区间中元素数目等分区间
    """

    def get_child_section_size(self, avg, std_deviation, child_section_num=120):
        """
        根据平均值、标准差、区间数目获取区间大小
        :param avg:
        :param std_deviation:
        :param child_section_num:
        :return:
        """
        # 先算出置信区间为95的数值范围
        min_num = avg - 2 * std_deviation
        min_num = 0 if min_num < 0 else min_num
        max_num = avg + 2 * std_deviation

        # 计算需要划分的子区间的数目和range
        child_section_size = (max_num - min_num) / child_section_num
        return self.__optimize_section_size(child_section_size)

    def get_child_section_range_list(self, avg, std_deviation, section_num=0):
        """
        获取数值范围区间列表 [{to:100},{from:100,to:200},{from:200}]
        :param avg:
        :param std_deviation:
        :param section_num:
        :return:
        """

        def get_single_section_range(index_pos, total_num, section_size):
            """
            获取单个区间{from:100,to:200}
            """
            if index_pos == 0:
                return {'to': section_size}
            elif index_pos == total_num - 1:
                return {'from': section_size * index_pos}
            else:
                return {'from': section_size * index_pos, 'to': section_size * (index_pos + 1)}

        section_rate = config.get_value('consts/global/algorithm/section_range_rate') or 20
        section_num = section_num or config.get_value('consts/global/algorithm/price_section_num') or 6
        child_section_num = section_rate * section_num
        child_section_size = self.get_child_section_size(avg, std_deviation, child_section_num)
        return [get_single_section_range(index, child_section_num, child_section_size) for index in
                xrange(child_section_num)]

    def merge_child_sections(self, child_sections, total_doc_count, section_num=0, optimize=True):
        """
        将子区间合并
        :param child_sections:
        :param total_doc_count:
        :param section_num:
        :param optimize 是否优化区间
        :return:
        """

        def add_child_section(sub_child_sections, sub_doc_count, sub_section_num, merged_sections):
            if not sub_section_num or not sub_child_sections or not sub_doc_count:
                return merged_sections

            # 首先处理最后一个区间，可能大小、数目均无法满足要求，但是也必须全部收集返回
            if sub_section_num == 1:
                temp_sum_section = reduce(self.__apend_section, sub_child_sections)
                merged_sections.append(self.__clone_es_range_bucket(temp_sum_section))
                return merged_sections

            sum_section = None
            sum_doc_count = 0
            avg_doc_count = sub_doc_count / sub_section_num
            cur_index = 0
            for index in xrange(len(sub_child_sections)):
                cur_index = index
                sum_doc_count += sub_child_sections[index]['doc_count']
                if sum_doc_count < avg_doc_count:
                    sum_section = self.__apend_section(sum_section,
                                                       self.__clone_es_range_bucket(sub_child_sections[index]))
                    if index == len(sub_child_sections) - 1:
                        # 表明加到最后一个区间总的文档数仍然小于平均数，直接将所有区间相加返回
                        merged_sections.append(sum_section)
                        return merged_sections
                elif sum_doc_count == avg_doc_count:
                    merged_sections.append(
                        self.__apend_section(sum_section, self.__clone_es_range_bucket(sub_child_sections[index])))
                    sub_section_num -= 1
                    break
                else:
                    middle_doc_count = (sum_doc_count * 2 + sub_child_sections[index]['doc_count']) / 2
                    if not sum_section:
                        merged_sections.append(
                            self.__apend_section(sum_section, self.__clone_es_range_bucket(sub_child_sections[index])))
                    elif avg_doc_count <= middle_doc_count:
                        merged_sections.append(sum_section)
                        cur_index -= 1
                    else:
                        merged_sections.append(
                            self.__apend_section(sum_section, self.__clone_es_range_bucket(sub_child_sections[index])))
                    sub_section_num -= 1
                    break

            add_child_section(sub_child_sections[cur_index + 1:], sub_doc_count - merged_sections[-1]['doc_count'],
                              sub_section_num, merged_sections)
            return self.__remove_null_section(merged_sections, section_num)

        if not child_sections or not total_doc_count:
            return []
        section_num = section_num or config.get_value('consts/global/algorithm/price_section_num') or 6
        result_sections = add_child_section(child_sections, total_doc_count, section_num, [])
        return result_sections


    def __optimize_section_size(self, section_size):
        """
        修改价格区间范围，比如算下来是11.5，这样显然是不合适的，需要改成10
        区间的分布为10，30，50，100，200，300，400，。。。，区间中间值一律取最靠近的值
        11.5的取值为10
        :param section_size:
        :return:
        """
        opt_ranges = config.get_value('consts/global/algorithm/price_section_opt_range') or [10, 20, 30, 50, 100]
        if section_size <= 20:
            return 10
        for index in xrange(len(opt_ranges)):
            if section_size > opt_ranges[index]:
                continue
            elif section_size == opt_ranges[index]:
                return opt_ranges[index]
            else:
                if index == 0:
                    return opt_ranges[index]
                middle_value = (opt_ranges[index] + opt_ranges[index - 1]) / 2
                return opt_ranges[index - 1] if section_size <= middle_value else opt_ranges[index]

        # 表示大于最大范围
        return round(float(section_size) / 100) * 100

    def __clone_es_range_bucket(self, es_range_bucket):
        """
        克隆ES range agg结果，保留：from，to，doc_count
        :param es_range_bucket:
        :return:
        """
        clone_obj = {}
        if 'from' in es_range_bucket:
            clone_obj['from'] = es_range_bucket['from']
        if 'to' in es_range_bucket:
            clone_obj['to'] = es_range_bucket['to']
        if 'doc_count' in es_range_bucket:
            clone_obj['doc_count'] = es_range_bucket['doc_count']
        return clone_obj

    def __apend_section(self, dst_section, src_section):
        """
        将两个区间相加，
        :param dst_section:
        :param src_section:
        :return:
        """
        if not dst_section:
            return src_section
        dst_section['doc_count'] = dst_section['doc_count'] + src_section['doc_count']
        if 'to' in src_section:
            dst_section['to'] = src_section['to']
        else:
            del dst_section['to']
        return dst_section

    def __remove_null_section(self, section_list, section_num):
        """
        消除文档数目为0的区间，和下一级合并
        :param section_list:
        :return:
        """
        filter_section_list = filter(lambda section: section['doc_count'] > 0, section_list)
        filter_section_list_length = len(filter_section_list)
        if filter_section_list_length == section_num:
            # 没有文档数目为0的空间，不需要进行整理
            return section_list
        for index in xrange(filter_section_list_length):
            if index == 0:
                # 第一个区间
                self.__delete_prop(filter_section_list[index], 'from')
            elif index == filter_section_list_length - 1:
                # 最后一个区间
                self.__delete_prop(filter_section_list[index], 'to')
            else:
                self.__set_prop(filter_section_list[index], 'from', filter_section_list[index - 1]['to']) \
                    if filter_section_list[index]['from'] != filter_section_list[index - 1]['to'] else ''
                self.__set_prop(filter_section_list[index], 'to', filter_section_list[index + 1]['from']) \
                    if filter_section_list[index]['to'] != filter_section_list[index + 1]['from'] else ''
        return filter_section_list

    def __delete_prop(self, obj, key):
        """
        删除字典中得元素
        :param obj:
        :param key:
        :return:
        """
        if not obj or not key:
            return
        if key in obj:
            del obj[key]

    def __set_prop(self, obj, key, value):
        """
        设置字典中元素的值
        :param obj:
        :param key:
        :param value:
        :return:
        """
        if not obj or not key:
            return
        obj[key] = value


equal_section_partitions = EqualSectionPartitions()

if __name__ == '__main__':
    print equal_section_partitions.get_child_section_range_list(1194.8092915980228, 2208.380040653766)
