# coding=utf-8
from math import sqrt

from algorithm.keyword_freq_service import keyword_freq_service
from common.utils import get_dict_value_by_path, bind_variable, bind_dict_variable
from common.pingyin_utils import pingyin_utils
from common.loggers import app_log, debug_log


__author__ = 'liuzhaoming'


class DataProcessing(object):
    @debug_log.debug('DataProcessing.process_data')
    def process_data(self, processing_config, data, suggest_config=None):
        """
        整理数据，生成可以插入到数据源中得数据
        :param processing_config:
        :param data:
        :param params:
        :param suggest_config:
        :return:
        """
        app_log.info('Process data is called')
        if not processing_config:
            return data
        processing_type = processing_config.get('type', 'basic_processing')

        _processing = Data_Processing_DICT.get(processing_type)
        if not _processing:
            app_log.error("Unsupport processing type:{0}", processing_config)
            return data
        return _processing.process_data(processing_config, data, suggest_config)


class BasicElasticsearchDataProcessing(DataProcessing):
    def process_data(self, processing_config, input_data, suggest_config=None):
        """
        整理数据，生成可以插入到ES数据源中得数据
        :param processing_config:
        :param input_data:
        :param suggest_config:
        :return:
        """
        if 'output' not in processing_config:
            app_log.error("output not in process_config {0}", processing_config)
            return None
        if not input_data or not input_data.get('root'):
            app_log.warning("input data is null")
            return None
        data_list = input_data['root']
        es_bulk_body_list = []
        word_list = map(lambda _data: _data['word'], data_list)
        word_query_freq_list = []
        if data_list:
            admin_id = data_list[0]['adminId']
            word_query_freq_list = keyword_freq_service.get_keyword_freq(admin_id, word_list)
        index = 0
        for data in data_list:
            data['id'] = self.__to_suggest_doc_id(data['word'])
            weight_update_body = self.__get_weight_update_body(processing_config, data, word_query_freq_list[index])
            index += 1
            payloads_update_body = self.__get_payload_update_body(processing_config, data)
            common_fields_update_body = self.__get_common_fields_update_body(processing_config, data)
            word = data['word']
            suggest_field = 'suggest-{0}'.format(data['doc_type'].lower())
            create_doc = {'name': word, suggest_field: {'payload': {}, 'weight': 0,
                                                    'input': pingyin_utils.get_pingyin_combination(word) + [
                                                        word], 'output': word}}

            update_doc = None
            if common_fields_update_body:
                for key in common_fields_update_body:
                    create_doc[key] = common_fields_update_body[key]
            if payloads_update_body:
                for key in payloads_update_body:
                    create_doc[suggest_field]['payload'][key] = payloads_update_body[key]
            if weight_update_body:
                if 'weight' in weight_update_body:
                    create_doc[suggest_field]['weight'] = weight_update_body['weight']
                else:
                    update_doc = weight_update_body
            es_bulk_body_list.append((create_doc, data))
            if update_doc:
                es_bulk_body_list.append((update_doc, data))
        return es_bulk_body_list

    def __get_weight_update_body(self, processing_config, data, word_query_freq=0):
        """
        获取权重更新数据
        :param processing_config:
        :param data:
        :param word_query_freq: 关键词搜索次数
        :return:
        """
        weight_config = get_dict_value_by_path('/output/weight', processing_config)
        if not weight_config:
            app_log.info("There is no weight config {0}", processing_config)
            return {'weight': 0}

        computer_type = weight_config.get('type', 'regex')
        if computer_type == 'regex':
            weight_value = float(bind_variable(weight_config['expression'], data))
            return {'weight': weight_value}
        elif computer_type == 'hits':
            return {
                'weight': int(sqrt(data['hits']['default']) + int(sqrt(word_query_freq)) + data['source_type_weight'])}
        elif computer_type == 'script':
            language = weight_config.get('language', 'mvel')
            if language != 'mvel':
                app_log.info("not support script language {0}", language)
                return {'weight': 0}
            result = {'script': weight_config['script'], 'detect_noop': True}
            if 'param' in weight_config and weight_config['param']:
                if weight_config['param'].get('type') == 'math_expression':
                    script_param_list = [(key, eval(value, None, data)) for (key, value) in
                                         weight_config['param']['fields'].iteritems()]
                    result['params'] = dict(script_param_list)
            return result

    def __get_payload_update_body(self, processing_config, data):
        """
        获取payload更新数据
        :param processing_config:
        :param data:
        :return:
        """
        payload_config = get_dict_value_by_path('/output/payloads', processing_config)
        if not payload_config:
            return {}
        computer_type = payload_config.get('type', 'regex')
        fields_config = payload_config.get('fields')

        return self.__bind_fields_config(computer_type, fields_config, data)

    def __get_common_fields_update_body(self, processing_config, data):
        """
        绑定suggest中的基础数据
        :param processing_config:
        :param data:
        :return:
        """
        common_fields_config = get_dict_value_by_path('/output/common_fields', processing_config)
        if not common_fields_config:
            return {}
        computer_type = common_fields_config.get('type', 'regex')
        fields_config = common_fields_config.get('fields')

        return self.__bind_fields_config(computer_type, fields_config, data)


    def __bind_fields_config(self, computer_type, fields_config, data):
        """
        绑定变量
        :param computer_type:
        :param fields_config:
        :param data:
        :return:
        """
        result = {}
        if not fields_config:
            return result
        if computer_type == 'map':
            for field in fields_config:
                result[field] = data[fields_config[field]]
        elif computer_type == 'regex':
            result = bind_dict_variable(fields_config, data)
        return result

    def __to_suggest_doc_id(self, word):
        reg = repr(word.decode('utf8'))
        return reg[2:(len(reg) - 1)]


Data_Processing_DICT = {'basic_processing': BasicElasticsearchDataProcessing()}
data_processing = DataProcessing()