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
    sys.path.append(os.path.join(os.path.dirname(__file__).replace('\\', '/'), '../'))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'search_platform.settings'

    import django

    django.setup()


init_django_env()

import time

from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from common.configs import config
from common.distributed_locks import distributed_lock
from common.loggers import app_log
from common.msg_bus import message_bus, Event
from common.utils import get_dict_value_by_path
from measure.measure_units import measure_unit_helper
from measure.measurements import measurement_helper
from measure.sourth import MeasureProcessor


def apscheduler_listener(event):
    """
    apscheduler监听器
    :param event:
    :return:
    """
    if event.exception:
        app_log.error('The job crashed : {0}', event.job_id)
        app_log.exception(event.exception)
    else:
        app_log.info('The job worked : {0}', event.job_id)


class MeasureMain(object):
    scheduler = BackgroundScheduler()
    processor = MeasureProcessor()

    def __init__(self):
        message_bus.add_event_listener(Event.TYPE_CONFIG_UPDATE, self.on_config_update_event)

    def start(self):
        """
        启动产品建议任务
        """
        app_log.info('Measure app begin starting')
        task_config_list = config.get_value('measure/measure_task/')
        index = 0
        for task_config in task_config_list:
            try:
                measure_unit_helper.merge_task_period_cfg(task_config)
                period_cfg = get_dict_value_by_path('period/sample_period', task_config)
                period_type = period_cfg['type'] if 'type' in period_cfg else 'interval'
                if period_type == 'crontab':
                    trigger = CronTrigger(year=period_cfg['year'] if 'year' in period_cfg else None,
                                          month=period_cfg['month'] if 'month' in period_cfg else None,
                                          day=period_cfg['day'] if 'day' in period_cfg else None,
                                          week=period_cfg['week'] if 'week' in period_cfg else None,
                                          day_of_week=period_cfg[
                                              'day_of_week'] if 'day_of_week' in period_cfg else None,
                                          hour=period_cfg['hour'] if 'hour' in period_cfg else None,
                                          minute=period_cfg['minute'] if 'minute' in period_cfg else None,
                                          start_date=period_cfg['start_date'] if 'start_date' in period_cfg else None,
                                          end_date=period_cfg['end_date'] if 'end_date' in period_cfg else None,
                                          timezone=period_cfg['timezone'] if 'timezone' in period_cfg else None)
                else:
                    trigger = IntervalTrigger(
                        minutes=period_cfg['total'] if 'total' in period_cfg else 24 * 60,
                        start_date=period_cfg[
                            'start_date'] if 'start_date' in period_cfg else None,
                        end_date=period_cfg['end_date'] if 'end_date' in period_cfg else None,
                        timezone=period_cfg['timezone'] if 'timezone' in period_cfg else None)
                if trigger:
                    app_log.info('Add measure task job : {0}', period_cfg)
                    task_name = task_config['name'] if 'name' in task_config else str(time.time())
                    if period_type == 'crontab':
                        process_fun = distributed_lock.lock(task_name)(self.processor.sample)
                    else:
                        minutes = period_cfg['total'] if 'total' in period_cfg else 24 * 60
                        task_lock_timeout = int(minutes * 0.9) if minutes < 30 else (minutes - 5)
                        process_fun = distributed_lock.lock(task_name, task_lock_timeout * 60, False)(
                            self.processor.sample)
                    measurements = measurement_helper.get_measurements_by_fingerprint(task_config)
                    self.scheduler.add_job(process_fun, args=[task_config, measurements], trigger=trigger,
                                           id=('measure_task_' + task_name))
            except Exception as e:
                app_log.error('Measure app has error, suggest river is {0}', e, task_config)
            index += 1
            self.scheduler.add_listener(apscheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        try:
            self.scheduler.start()
        except Exception as e:
            app_log.error("Measure app start has error ", e)
        app_log.info('Measure app started')

    def stop(self):
        """
        停止产品建议任务
        """
        try:
            app_log.info('Measure app begin stopping')
            self.scheduler.remove_all_jobs()
            self.scheduler.remove_listener(apscheduler_listener)
            self.scheduler.shutdown()
            app_log.info('Measure app stopped')
        except Exception as e:
            app_log.exception(e)

    def on_config_update_event(self):
        """
        响应配置数据变更事件
        """
        self.stop()
        self.start()


main_app = MeasureMain()

if __name__ == '__main__':
    main_app.start()

    while True:
        time.sleep(10)