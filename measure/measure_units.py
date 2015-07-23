# coding=utf-8
from common.configs import config
from common.exceptions import InvalidParamError
from common.loggers import app_log
from common.utils import deep_merge

__author__ = 'liuzhaoming'


class MeasureUnitHelper(object):
    period_cfg_cache = {}

    def get_measure_unit_cfg(self, measure_unit_name):
        """
        获取测量单元配置
        :param measure_unit_name:
        :return:
        """
        measure_units = config.get_value('/measure/measure_unit')
        filter_measure_units = filter(lambda measure_unit_cfg: measure_unit_cfg['name'] == measure_unit_name,
                                      measure_units)
        if filter_measure_units:
            return filter_measure_units[0]
        return None

    def merge_task_period_cfg(self, task_cfg):
        """
        整合task 周期配置
        :param task_cfg:
        :return:
        """
        period_cfg = dict(task_cfg.get('period')) or {'type': 'interval', 'sample_period': {'days': 1}}
        period_type = period_cfg.get('type') or 'interval'
        if period_type == 'crontab':
            sample_period_cfg = deep_merge(period_cfg.get('sample_period'), period_cfg.get('measure_period'))
            sample_period_cfg['sample_multiples'] = 1
            period_cfg['sample_period'] = sample_period_cfg
        elif not period_cfg.get('sample_period') or not period_cfg.get('measure_period'):
            period_cfg['sample_period'] = period_cfg.get('measure_period') or period_cfg.get('sample_period')
            period_cfg['sample_period']['total'] = self.__calculate_period(period_cfg['sample_period'])
            period_cfg['sample_period']['sample_multiples'] = 1
        else:
            measure_period_total = self.__calculate_period(period_cfg['measure_period'])
            sample_period_total = self.__calculate_period(period_cfg['sample_period'])

            if int(measure_period_total) % int(sample_period_total) != 0:
                app_log.error(
                    'Measure Period should be an integral multiple of Sample, measure_period={0}, sample_period={1}',
                    period_cfg['measure_period'], period_cfg['sample_period'])
                raise InvalidParamError('Measure Period should be an integral multiple of Sample Period')
            sample_multiples = int(measure_period_total) / int(sample_period_total)
            sample_period_cfg = deep_merge(period_cfg.get('sample_period'), period_cfg.get('measure_period'))
            sample_period_cfg['total'] = sample_period_total
            sample_period_cfg['sample_multiples'] = sample_multiples
            period_cfg['sample_period'] = sample_period_cfg
        task_cfg['period'] = period_cfg
        return period_cfg

    def get_task_period_cfg(self, task_name):
        """
        获取性能任务周期配置信息
        :param task_name:
        :return:
        """
        if task_name in self.period_cfg_cache:
            return self.period_cfg_cache[task_name]
        task_cfg = self.get_task_cfg(task_name)
        if not task_cfg:
            raise InvalidParamError('Cannot find measure task config by task name: {0}'.format(task_name))
        period_cfg = self.merge_task_period_cfg(task_cfg)
        self.period_cfg_cache[task_name] = period_cfg
        return period_cfg

    def get_task_cfg(self, task_name):
        """
        根据task名称获取task配置信息
        :param task_name:
        :return:
        """
        task_cfg_list = config.get_value('/measure/measure_task')
        for task_cfg in task_cfg_list:
            if task_cfg['name'] == task_name:
                return task_cfg


    def __calculate_period(self, period):
        """
        以分钟为单位计算周期
        :param period:
        :return:
        """
        weeks = period.get('weeks') or 0
        days = period.get('days') or 0
        hours = period.get('hours') or 0
        minutes = period.get('minutes') or 0
        period_time = ((weeks * 7 + days) * 24 + hours) * 60 + minutes
        if period_time == 0:
            raise InvalidParamError('Period is 0 minute')
        return period_time


measure_unit_helper = MeasureUnitHelper()