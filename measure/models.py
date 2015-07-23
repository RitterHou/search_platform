# coding=utf-8
from itertools import chain
import json
from datetime import datetime

import redis

from common.adapter import es_adapter

from common.configs import config
from common.connections import EsConnectionFactory
from search_platform import settings
from common.utils import get_default_es_host, get_time_by_mill, get_time_by_mill_str


__author__ = 'liuzhaoming'

redis_conn = redis.Redis(connection_pool=redis.ConnectionPool.from_url(settings.SERVICE_BASE_CONFIG['redis']))


class MeasureCycleData(object):
    """
    单个周期内的测量数据
    """

    def __init__(self, task_cfg=None, **args):
        self.task_name = task_cfg['name'] if task_cfg else ''
        self.sample_data_list = []
        self.start_time = get_time_by_mill_str()
        self.measure_task_result = []
        self.measure_sample_result = []
        self.end_time = None
        self.__dict__.update(args)


    @staticmethod
    def from_json_str(json_str, task_cfg):
        """
        从json 字符串生成测量数据对象
        :param json_str:
        :param task_cfg:
        :return:
        """
        if not json_str:
            return MeasureCycleData(task_cfg)
        else:
            return MeasureCycleData(**json.loads(json_str))

    def to_json_str(self):
        """
        将测量数据对象转换为json 字符串
        :return:
        """
        return json.dumps(self, default=lambda o: o.__dict__)

    @staticmethod
    def from_cache(task_cfg):
        """
        从缓存中读取当前测量结果对象
        :param task_cfg:
        :return:
        """
        task_name = task_cfg['name']
        last_measure_data_str = redis_conn.hget(task_name, 'current')
        return MeasureCycleData.from_json_str(last_measure_data_str, task_cfg)


    def add_sample_data(self, task_cfg, sample_data):
        """
        增加采样点数据，如果本周期的采样数据已满则返回-1;如果加上本采样数据后已满返回0，如果加上本采样数据仍未满返回1
        :param task_cfg:
        :param sample_data:
        :return:
        """
        sample_multiples = task_cfg['period']['sample_period']['sample_multiples']
        if not self.__is_last_sample(task_cfg):
            self.sample_data_list.append(sample_data)
            return 0 if len(self.sample_data_list) == sample_multiples else 1
        else:
            # 表示本测量周期里面的采样点已满，该采样点应该属于下一个周期，返回False
            return -1

    def save(self, task_cfg):
        """
        持久化保存数据对象到ES
        :param task_cfg:
        :return:
        """
        host, index, doc_type, id = self.__get_es_store_info(task_cfg)
        es_conn = EsConnectionFactory.get_es_connection(host)
        bulk_body = list(chain(
            *map(lambda obj_measure_result: ({'index': {}}, obj_measure_result), self.measure_task_result)))
        es_bulk_result = es_conn.bulk(index=index, doc_type=doc_type, body=bulk_body)
        es_adapter.process_es_bulk_result(es_bulk_result)

        if self.__is_save_sample_data(task_cfg):
            host, index, doc_type, id = self.__get_es_store_info(task_cfg, '/consts/measure/es/sample/')
            es_conn = EsConnectionFactory.get_es_connection(host)
            bulk_body = list(chain(
                *map(lambda obj_measure_result: ({'index': {}}, obj_measure_result), self.measure_sample_result)))
            es_bulk_result = es_conn.bulk(index=index, doc_type=doc_type, body=bulk_body)
            es_adapter.process_es_bulk_result(es_bulk_result)


    def save_cache(self, task_cfg, measurements):
        """
        保存对象到Redis缓存中
        :param task_cfg:
        :param measurements:
        :return:
        """
        if not self.__is_last_sample(task_cfg):
            redis_conn.hset(self.task_name, 'current', self.to_json_str())
        else:
            self.__calculate_cycle_data(task_cfg, measurements)
            self.end_time = get_time_by_mill_str()
            redis_conn.hset(self.task_name, 'last', self.to_json_str())
            redis_conn.hset(self.task_name, 'current', MeasureCycleData(task_cfg).to_json_str())
            self.save(task_cfg)

    def __is_last_sample(self, task_cfg):
        """
        判断是否是最后一个采样点
        :param task_cfg:
        :return:
        """
        sample_multiples = task_cfg['period']['sample_period']['sample_multiples']
        return len(self.sample_data_list) == sample_multiples

    def __calculate_cycle_data(self, task_cfg, measurements):
        """
        计算周期中的测量数据,数据采集分为两种，sub类型的必须要到第二个周期才可以计算出数据
        :param task_cfg:
        :param measurements:
        :return:
        """
        if self.__is_last_sample(task_cfg):
            last_measure_data_str = redis_conn.hget(self.task_name, 'last')
            if not last_measure_data_str:
                # 上个测量周期的数据不存在，表示是第一次执行数据
                last_measure_sample_list = ()
            else:
                last_measure_cycle_data = MeasureCycleData.from_json_str(last_measure_data_str, task_cfg)
                last_measure_sample_list = last_measure_cycle_data.sample_data_list
            measurements = list(chain(*map(lambda fingerprint: measurements[fingerprint], measurements)))
            # 根据采样数据计算周期内测试结果
            self.measure_task_result = []
            for measure_obj_key in self.sample_data_list[0]:
                cur_obj_measure_result = {'@obj_key': measure_obj_key}
                for measurement in measurements:
                    measurement_value = measurement.cal_value(self.sample_data_list, last_measure_sample_list,
                                                              measure_obj_key)
                    cur_obj_measure_result[measurement.cfg['name']] = measurement_value
                cur_obj_measure_result['@collect_time'] = get_time_by_mill_str()
                self.measure_task_result.append(cur_obj_measure_result)
            # 根据任务设置和全局设置保存采样数据到ES中
            if self.__is_save_sample_data(task_cfg):
                self.measure_sample_result = []
                for sample_data in self.sample_data_list:
                    for measure_obj_key in sample_data:
                        cur_obj_sample_result = dict(sample_data[measure_obj_key])
                        cur_obj_sample_result['@obj_key'] = measure_obj_key
                        self.measure_sample_result.append(cur_obj_sample_result)


    def __get_es_store_info(self, task_cfg, path_prex='/consts/measure/es/task/'):
        """
        获取性能保存到ES中的任务相关信息
        :param task_cfg:
        :return:
        """
        index_template = config.get_value(path_prex + 'index')
        type_template = config.get_value(path_prex + 'type')
        id_template = config.get_value(path_prex + 'id')
        host = config.get_value(path_prex + 'host') or get_default_es_host()
        today = datetime.today()
        values = {'year': str(today.year), 'month': str(today.month), 'day': str(today.day),
                  'version': config.get_value('version'), 'task_name': task_cfg['name'],
                  'time_stamp': str(get_time_by_mill())}
        return host, index_template.format(**values), type_template.format(**values), id_template.format(**values)

    def __is_save_sample_data(self, task_cfg):
        """
        是否保存采样数据
        :param task_cfg:
        :return:
        """
        return task_cfg['save_sample_data'] if 'save_sample_data' in task_cfg else config.get_config(
            'consts/measure/save_sample_data')


