# coding=utf-8
from itertools import imap

from re import search

from common.connections import EsConnectionFactory
from common.exceptions import InvalidParamError, GenericError
from common.utils import get_default_es_host, deep_merge
from measure.measure_units import measure_unit_helper


__author__ = 'liuzhaoming'


class MeasureObj(object):
    """
    测量对象
    """

    def get_objs(self, measure_obj_cfg):
        """
        获取测量对象
        :param measure_obj_cfg:
        :return:
        """
        pass

    def get_key(self):
        """
        获取测量对象主键
        :return:
        """
        pass


class EsIndexMeasureObj(MeasureObj):
    """
    ES索引测量对象
    """

    def __init__(self, **args):
        self.host = args.get('host')
        self.index = args.get('index')

    def __unicode__(self):
        return 'EsIndexMeasureObj(host={0}, index={1})'.format(self.host, self.index)

    def __getitem__(self, item):
        if item == 'host':
            return self.host
        if item == 'index':
            return self.index
        raise InvalidParamError('Unsupport property name {0}'.format(item))

    def __setitem__(self, key, value):
        if key == 'host':
            self.host = value
        if key == 'index':
            self.index = value
        raise InvalidParamError('Unsupport property name {0}'.format(key))

    def get_key(self):
        return self.index

    def get_objs(self, measure_obj_cfg):
        es_host = measure_obj_cfg.get('host') or get_default_es_host()
        es_connection = EsConnectionFactory.get_es_connection(es_host)
        filter_cfg = measure_obj_cfg.get('filter')
        es_index_list = self.__get_indexs(es_connection, filter_cfg)
        return es_index_list

    def __get_indexs(self, es_connection, filter_cfg):
        """
        获取ES 集群中得所有索引，通过get index 别名的方式获取，因为别名最少最快
        :param es_connection:
        :return:
        """
        es_index_info_dict = es_connection.indices.get('_all', ['_aliases'])
        host_str = ','.join(es_connection.host_list)
        es_index_list = imap(lambda es_index_info: {'index': es_index_info, 'host': host_str},
                             es_index_info_dict.iterkeys())
        return [EsIndexMeasureObj(**es_index) for es_index in es_index_list if
                self.__match_conditions(es_index, filter_cfg)]

    def __match_conditions(self, data, filter_cfg):
        """
        判断是否符合所有匹配条件
        """
        union_operator = filter_cfg.get('union_operator') or 'and'
        condition_list = filter_cfg.get('conditions') or []
        for condition in condition_list:
            match_result = self.__match_single_condition(data, condition)
            if union_operator == 'and' and not match_result:
                return False
            elif union_operator == 'or' and match_result:
                return True
        return True if union_operator == 'and' else False

    def __match_single_condition(self, data, condition):
        """
        判断是否符合单个匹配条件
        """
        operator = condition['operator']
        match_result = False
        if condition['type'] == 'regex':
            # 正则表达式匹配条件
            value = data[condition['field']]
            msg_text = str(value)
            match_result = True if search(condition['expression'], msg_text) else False
            match_result = not match_result if operator == 'not' else match_result
        elif condition['type'] == 'equal':
            # 正则表达式匹配条件
            value = data[condition['field']]
            msg_text = str(value)
            match_result = condition['expression'] == msg_text
            match_result = not match_result if operator == 'not' else match_result
        return match_result


class MeasureObjHelper(object):
    """
    测量对象操作类
    """
    OBJ_DICT = {'es_index': EsIndexMeasureObj()}

    def get_objs(self, measure_obj_cfg):
        """
        获取测量对象
        :param measure_obj_cfg:
        :return:
        """
        if not measure_obj_cfg:
            raise InvalidParamError('Measure obj config is null')
        obj_type = measure_obj_cfg['type']
        measure_obj = self.OBJ_DICT.get(obj_type)
        if not measure_obj:
            raise InvalidParamError('Cannot find measure obj')
        return measure_obj.get_objs(measure_obj_cfg)

    def get_task_objs(self, task_cfg):
        """
        获取测量对象
        :param task_cfg:
        :return:
        """
        if not task_cfg:
            raise InvalidParamError('Task config is null')
        measure_unit_name = task_cfg['measure_unit_name']
        measure_unit_cfg = measure_unit_helper.get_measure_unit_cfg(measure_unit_name)
        if not measure_unit_cfg:
            raise GenericError('Measure Unit Config is null {0}'.format(measure_unit_name))
        measure_obj_cfg = deep_merge(measure_unit_cfg.get('measure_obj'), task_cfg.get('measure_obj'))
        return self.get_objs(measure_obj_cfg)


measure_obj_helper = MeasureObjHelper()
