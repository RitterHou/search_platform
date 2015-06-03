# coding=utf-8
import json

__author__ = 'liuzhaoming'

from itertools import *


def parse_skuids_from_pc_mq_msg(pc_ids, sep_char=','):
    """
    从商品中心的变更消息的id中解析出skuid，商品中心的消息ID格式为：
    '21182:g4903;21224:g4989;'  ,  '21224:g4989;'  ,  '21182:g4903:g4943;21188:g4951;21224:g4989;'
    :param pc_ids:
    :return: 'g4903,g4989,g4943'
    """

    def __parse_spu_group(spu_group_id):
        """
        从spu的分组id中获取skuid
        :param spu_group_ids: 格式为21182:g4903:g4943
        :return: ('g4903','g4943')
        """
        temp_id_list = spu_group_id.split(':')
        return temp_id_list[1:] if len(temp_id_list) > 1 else ()

    if not pc_ids:
        return ''
    spu_groups = pc_ids.split(';')
    skuid_list = chain(*map(__parse_spu_group, spu_groups))
    return sep_char.join(skuid_list)


def parse_cateIds_from_standard_cate_update_msg(msg_text):
    """
    从标准商品标准类目变更消息中解析出cateIDs
            {
          "cateIds": [
            "c264",
            "c265",
            "c269"
          ],
          "status": 1,
          "type": "updateStatus"
        }
    :param msg_text:
    :return:
    """
    if not msg_text:
        return None
    json_result = json.loads(msg_text)
    if 'cateIds' not in json_result:
        return None
    return ','.join(json_result['cateIds'])


if __name__ == '__main__':
    print parse_skuids_from_pc_mq_msg('21182:g4903;21224:g4989;')
    print parse_skuids_from_pc_mq_msg('21182:g4903;')
    print parse_skuids_from_pc_mq_msg('21182:g4903:g4943;21188:g4951;21224:g4989;')
    print parse_skuids_from_pc_mq_msg('21182')