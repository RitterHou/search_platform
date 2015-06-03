# coding=utf-8
__author__ = 'liuzhaoming'


def init_django_env():
    """
    初始化Django环境
    :return:
    """
    import sys
    import os

    sys.path.append(os.path.dirname(__file__).replace('\\', '/'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../../'))
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../../../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()

import time

from common.utils import unbind_variable


work_spend_time_regex = 'celery process message spend (?P<value>[\\d\.]+)'
es_spend_time_regex = 'es spend time (?P<value>[\\d\.]+)'
start_time_regex = 'INFO (?P<value>[\\d,: -]+) rivers.py\[line:22\]'
end_time_regex = 'INFO (?P<value>[\\d,: -]+) rivers.py\[line:30\]'


class Statistic(object):
    def static_worker(self, *args):
        es_spend_time, work_spend_time, start_time, end_time = 0, 0, 0, 0
        total_average_time = 0
        for file_name in args:
            cur_es_spend_time, cur_work_spend_time, cur_start_time, cur_end_time = self.__static_worker_file(file_name)
            es_spend_time += cur_es_spend_time
            work_spend_time += cur_work_spend_time
            cur_start_time_stamp = self.__parse_time(cur_start_time)
            cur_end_time_stamp = self.__parse_time(cur_end_time)
            if not start_time or start_time > cur_start_time_stamp:
                start_time = cur_start_time_stamp
            if not end_time or end_time < cur_end_time_stamp:
                end_time = cur_end_time_stamp
            total_average_time += (end_time - start_time) / 5000 * len(args)
        total_time = end_time - start_time
        es_average_time = es_spend_time / 5000
        search_platform_average_time = work_spend_time / 5000 - es_average_time
        total_average_time /= len(args)
        celery_schedule_average_time = total_average_time - work_spend_time / 5000
        capacity = 5000 / total_time
        es_percent = es_average_time / total_average_time
        work_percent = work_spend_time / total_time
        search_platform_percent = search_platform_average_time / total_average_time
        celery_schedule_percent = celery_schedule_average_time / total_average_time
        print '系统吞吐量:{0} , 总时间:{1} , 单次操作平均总时间:{2} , 搜索引擎自身操作平均时间:{3} , ES操作平均时间:{4} , 任务调度平均时间:{5} , ' \
              '搜索引擎耗时百分比:{6}% , ES操作耗时百分比:{7}% , 任务调度耗时百分比:{8}%'.format(capacity, total_time, total_average_time,
                                                                        search_platform_average_time, es_average_time,
                                                                        celery_schedule_average_time,
                                                                        search_platform_percent * 100, es_percent * 100,
                                                                        celery_schedule_percent * 100)


    def __static_worker_file(self, file):
        f = open(file)
        work_spend_time = 0
        es_spend_time = 0
        start_time = 0
        for line in f:
            if not line:
                continue

            key, work_spend_time_str = unbind_variable(work_spend_time_regex, 'value', line)
            if work_spend_time_str:
                work_spend_time += float(work_spend_time_str)

            key, es_spend_time_str = unbind_variable(es_spend_time_regex, 'value', line)
            if es_spend_time_str:
                es_spend_time += float(es_spend_time_str)

            if not start_time:
                key, current_start_time = unbind_variable(start_time_regex, 'value', line)
                if current_start_time:
                    # 2015-04-29 14:39:38,993
                    start_time = current_start_time

            key, current_end_time = unbind_variable(end_time_regex, 'value', line)
            if current_end_time:
                # 2015-04-29 14:39:38,993
                end_time = current_end_time

        return es_spend_time, work_spend_time, start_time, end_time

    def __parse_time(self, time_str):
        """
        解析时间，返回毫秒：‘2015-04-29 14:39:38,993’
        :param time_str:
        :return:
        """
        temp_strs = time_str.split(',')
        struct_time = time.strptime(temp_strs[0], '%Y-%m-%d %H:%M:%S')
        time_stamp = time.mktime(struct_time)
        time_stamp += float(temp_strs[1]) / 1000
        return time_stamp


if __name__ == '__main__':
    statistic = Statistic()
    statistic.static_worker('/Users/liuzhaoming/project/python/django/search_platform/logs/root.log')
