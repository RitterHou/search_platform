# -*- coding: utf-8 -*-
import json

from common.scripts import python_invoker
from common.utils import get_dict_value_by_path, unbind_variable, COMBINE_SIGN, hash_encode
from common.loggers import app_log

__author__ = 'liuzhaoming'


class DataParser(object):
    """
    数据解析器
    """

    def parse(self, data, parser_config):
        """
        解析数据
        :param data:
        :param parser_config:
        :return:
        """
        parse_result = {}
        parser_type = get_dict_value_by_path('type', parser_config)
        for fields_name in parser_config:
            if fields_name.startswith('fields'):
                fields_config = parser_config.get(fields_name)
                iter_parse_result = {}
                for field_name, item_config in fields_config.iteritems():
                    field_key, field_value = item_parser.parse_item(data, item_config, field_name, iter_parse_result,
                                                                    parser_type)
                    if field_value is not None:
                        iter_parse_result[field_key] = field_value

                parse_result[fields_name] = iter_parse_result
        return parse_result


class ItemParser(object):
    def parse_item(self, data, item_config, field_name, parse_result, parent_parser_type=None):
        """
        解析单个字段
        """
        if isinstance(item_config, str) or isinstance(item_config, unicode):
            item_config = {'expression': item_config, 'type': parent_parser_type}
        item_config_key = self.__get_item_parser_cfg_key(item_config, parent_parser_type)
        _item_parser = _ITEM_PARSER_CONTAINER.get(item_config_key)
        if not _item_parser:
            app_log.error('Cannot get item parser item_config={0} , field_name={1} , parent_parser_type={2}',
                          item_config, field_name, parent_parser_type)
            return field_name, None
        return _item_parser.parse_item(data, item_config, field_name, parse_result)

    @staticmethod
    def __get_item_parser_cfg_key(item_config, parent_parser_type):
        """
        获取属性解析器主键
        """
        parser_type = item_config.get('type') or parent_parser_type
        language = item_config.get('language')
        return COMBINE_SIGN.join((parser_type, language) if language else [parser_type])


class RegexItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        通过正则表达式解析单个字段
        """
        expression = item_config.get('expression')
        if not expression:
            app_log.warning('RegexItemParser parse item fail, because has not expression config, {0} , {1}',
                            item_config, field_name)
            return field_name, None
        if not isinstance(data, str):
            data = str(data)
        return unbind_variable(expression, field_name, data)


class DictItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        解析JSON的字典元素
        """

        dict_key = item_config.get('key') or field_name
        key, value = unbind_variable('"' + dict_key + '":{(?P<' + field_name + '>[\\d\\D]+?)}', field_name, data)
        return key, json.loads('{' + value + '}')


class PythonScriptItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        通过Python脚本解析字段
        """
        input_param = data
        parse_result = None
        # 解析脚本执行入参
        if 'input_param' in item_config:
            input_param_config = item_config['input_param']
            input_param = data_parser.parse(data, input_param_config).get('fields')

        try:
            parse_result = python_invoker.invoke(item_config, input_param)
        except Exception as e:
            app_log.error('Parse item error, data={0} , item_config={1}, field_name={2}', e, data, item_config,
                          field_name)
        return field_name, parse_result


class FixedItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        固定值
        """
        expression = item_config.get('expression')
        if not expression:
            app_log.warning('FixedItemParser parse item fail, because has not expression config, {0} , {1}',
                            item_config, field_name)
            return field_name, None
        return field_name, expression


class SourceItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        直接返回解析器输入值
        :param data:
        :param item_config:
        :param field_name:
        :return:
        """
        return field_name, data


class HashModulusItemParser(ItemParser):
    def parse_item(self, data, item_config, field_name, parse_result):
        """
        计算hash函数的取模
        :param data:
        :param item_config:
        :param field_name:
        :return:
        """
        modulus = item_config.get('modulus') or 10
        origin_field_name = item_config.get('origin_filed')
        hash_code = hash_encode(parse_result.get(origin_field_name), modulus)
        return field_name, hash_code


_ITEM_PARSER_CONTAINER = {
    'dict': DictItemParser(),
    'regex': RegexItemParser(),
    'fixed': FixedItemParser(),
    'source': SourceItemParser(),
    'script' + COMBINE_SIGN + 'python': PythonScriptItemParser(),
    'hash_modulus': HashModulusItemParser()
}

data_parser = DataParser()
item_parser = ItemParser()

if __name__ == '__main__':
    import time

    parser_config = {
        "type": "regex",
        "fields": {
            "adminId": "chainMasterId\":\"(?P<adminId>[\\d\\D]+?)\"",
            "ids": "\"ids\":\"(?P<ids>[\\d\\D]+?)\"",
            "skuids": {
                "type": "script",
                "language": "python",
                "obj_path": "script_data_parsers.parse_skuids_from_pc_mq_msg",
                "input_param": {
                    "fields": {
                        "pc_ids": {
                            "type": "regex",
                            "expression": "\"ids\":\"(?P<pc_ids>[\\d\\D]+?)\""
                        },
                        "sep_char": {
                            "type": "fixed",
                            "expression": ","
                        }
                    }
                }
            }
        }
    }
    start_time = time.time()
    i = 0
    for i in xrange(100000):
        data = '{"chainMasterId":"A857673","ids":"21182:g4903;21188:g4951;21224:g4989;","operation":"stock","sys":2,"type":"update"}'
        data_parser.parse(data, parser_config)
    print 'spend time : ' + str(time.time() - start_time)
    data = '{"chainMasterId":"A857673","ids":"21182:g4903;21188:g4951;21224:g4989;","operation":"stock","sys":2,"type":"update"}'
    print data_parser.parse(data, parser_config)
    data = '{"chainMasterId":"A857673","ids":"21182:g4903;","operation":"stock","sys":2,"type":"update"}'
    print data_parser.parse(data, parser_config)
