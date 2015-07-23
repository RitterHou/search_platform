# coding=utf-8
from itertools import ifilter
import json

from common.exceptions import InvalidParamError
from common.loggers import app_log
from common.utils import get_dict_value_by_path, deep_merge
from measure.measure_units import measure_unit_helper


__author__ = 'liuzhaoming'


class Measurement(object):
    """
    测量指标
    """

    def __init__(self, **measurement_cfg):
        self.cfg = measurement_cfg
        self.fingerprint = self.__build_fingerprint()

    def __build_fingerprint(self):
        measurement_type = self.cfg.get('type') or 'elasticsearch'
        operation = self.cfg.get('operation') or 'search'
        search_type = self.cfg.get('search_type') or ''
        return 'type={0}, operation={1}, search_type={2}'.format(measurement_type, operation, search_type)

    def get_sample_value(self, collect_result):
        """
        从单次采样结果中获取测量指标的值
        :param collect_result:
        :return:
        """
        value = None
        if self.fingerprint == 'type=elasticsearch, operation=stats, search_type=':
            value = get_dict_value_by_path(self.cfg['value_field'], collect_result)
        elif self.fingerprint == 'type=elasticsearch, operation=search, search_type=count':
            value_filed = self.cfg.get('value_field') or '/hits/total'
            value = get_dict_value_by_path(value_filed, collect_result)
        elif self.fingerprint.startswith('type=elasticsearch, operation=search, search_type=aggs'):
            value_filed = self.cfg.get('value_field')
            if not value_filed:
                value_filed = "/".join(("aggregations", self.cfg.get('field') + self.cfg.get('aggs_type'), "value"))
            value = get_dict_value_by_path(value_filed, collect_result)
        if value is not None:
            value_type = self.cfg.get('data_type') or 'int'
            if value_type == 'int':
                return value if isinstance(value, int) else int(value)
            else:
                return value if isinstance(value, float) else float(value)
        app_log.warning("Cannot find value from collect result, cfg={0}", self.cfg)

    def cal_value(self, sample_result_list, last_sample_result_list, measure_obj_key):
        """
        计算统计指标的值，丢点可以继续计算
        :param sample_result_list:
        :param last_sample_result_list:
        :param measure_obj_key:
        :return:
        """
        if not sample_result_list:
            app_log.error('sample_result_list is null')
            return
        calculate_policy = self.cfg['calculate_policy'] if 'calculate_policy' in self.cfg else 'max'
        sample_value_list = map(lambda sample_result: sample_result[measure_obj_key][self.cfg['name']],
                                ifilter(lambda item: item.get(measure_obj_key) and item.get(measure_obj_key).get(
                                    self.cfg['name']) is not None, sample_result_list))
        if not sample_value_list:
            return 0
        if calculate_policy == 'max':
            return max(sample_value_list)
        elif calculate_policy == 'min':
            return min(sample_value_list)
        elif calculate_policy == 'sum':
            return sum(sample_value_list)
        elif calculate_policy == 'avg':
            return sum(sample_value_list) / len(sample_value_list)
        elif calculate_policy == 'sub':
            if not last_sample_result_list:
                return 0
            elif self.cfg['name'] in last_sample_result_list[-1]:
                return sample_value_list[-1] - last_sample_result_list[-1][self.cfg['name']]
            else:
                return 0
        return 0


    def get_dsl(self, index_name):
        """
        获取 DSL
        :param index_name:
        :return:
        """
        measurement_type = self.cfg.get('type') or 'elasticsearch'
        if measurement_type != 'elasticsearch':
            return None
        operation = self.cfg.get('operation') or 'search'
        if operation != 'search':
            return None
        search_type = self.cfg.get('search_type') or ''
        if search_type == 'count':
            return self.__get_count_dsl(index_name)
        elif search_type == 'aggs':
            return self.__get_aggs_dsl(index_name)

    def __get_count_dsl(self, index_name):
        """
        获取 ES count 查询 DSL
        :param index_name:
        :return:
        """
        if not self.cfg.get('dsl'):
            return {'search_type': 'count', 'index': index_name}, {"query": {"match_all": {}}, "size": 0}
        else:
            count_dsl = json.loads(self.cfg.get('dsl'))
            if 'size' not in count_dsl:
                count_dsl['size'] = 0
            return {'search_type': 'count', 'index': index_name}, count_dsl

    def __get_aggs_dsl(self, index_name):
        """
        获取ES aggs 查询DSL
        :param index_name:
        :return:
        """
        aggs_dsl = self.cfg.get('dsl')
        if self.cfg.get('aggs_type'):
            if self.cfg.get('field'):
                field_agg_dsl = {"aggs": {self.cfg.get('field') + self.cfg.get('aggs_type'): {
                    self.cfg.get('aggs_type'): {"field": self.cfg.get('field')}}}}
                aggs_dsl = deep_merge(aggs_dsl, field_agg_dsl)
        if 'size' not in aggs_dsl:
            aggs_dsl['size'] = 0
        return {'search_type': 'count', 'index': index_name}, aggs_dsl


class MeasurementHelper(object):
    def get_measurements(self, task_cfg):
        """
        获取测量任务下的所有测量指标
        :param task_cfg:
        :return:
        """

        def find_measurement_by_name(measurement_name, measurement_list):
            """
            根据测试指标名称获取测量指标
            :param measurement_name:
            :param measurement_list:
            :return:
            """
            if not measurement_list:
                return None
            for measurement_cfg in measurement_list:
                if measurement_cfg['name'] == measurement_name:
                    return measurement_cfg

        if not task_cfg.get('measure_unit_name'):
            raise InvalidParamError('Task cfg is invalid because the measure_unit_name is null')
        measure_unit_cfg = measure_unit_helper.get_measure_unit_cfg(task_cfg.get('measure_unit_name'))
        if not task_cfg.get('measurements'):
            measurement_cfg_list = measure_unit_cfg.get('measurements')
        else:
            measurement_name_list = task_cfg.get('measurements')
            measurement_cfg_list = map(lambda measurement_name: find_measurement_by_name(
                measurement_name, measure_unit_cfg.get('measurements')), measurement_name_list)
            measurement_cfg_list = filter(lambda measurement_cfg: measurement_cfg, measurement_cfg_list)

        return map(lambda measure_cfg: Measurement(**measure_cfg), measurement_cfg_list)

    def get_measurements_by_fingerprint(self, task_cfg):
        """
        根据指纹对测量任务下的测量指标进行分类
        :param task_cfg:
        :return:
        """
        measurements = self.get_measurements(task_cfg)
        classify_measurements = {}
        for measurement in measurements:
            if measurement.fingerprint in classify_measurements:
                classify_measurements[measurement.fingerprint].append(measurement)
            else:
                classify_measurements[measurement.fingerprint] = [measurement]
        return classify_measurements


measurement_helper = MeasurementHelper()

