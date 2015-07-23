# coding=utf-8
from common.loggers import debug_log, app_log
from measure.collectors import collector_helper
from measure.measure_objs import measure_obj_helper
from measure.models import MeasureCycleData

__author__ = 'liuzhaoming'


class MeasureProcessor(object):
    def sample(self, task_cfg, measurements):
        """
        测量任务的执行，主要由采样、计算结果、保存数据
        :param task_cfg:
        :param measurements:
        :return:
        """
        try:
            debug_log.print_log('Measure sample is called {0}', task_cfg.get('name'))
            measure_objs = measure_obj_helper.get_task_objs(task_cfg)
            collect_result = collector_helper.collect(task_cfg, measure_objs)
            debug_log.print_log('Measure sample is finish {0} , result={1}', task_cfg.get('name'), collect_result)
            measure_cycle_data = MeasureCycleData.from_cache(task_cfg)
            measure_cycle_data.add_sample_data(task_cfg, collect_result)
            measure_cycle_data.save_cache(task_cfg, measurements)
        except Exception as e:
            app_log.error('Measure sample has error {0} , {1}', e, task_cfg, measurements)
