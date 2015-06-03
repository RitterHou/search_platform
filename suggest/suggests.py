# -*- coding: utf-8 -*-


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

from common.configs import config
from common.utils import get_dict_value_by_path
from suggest.notifications import SuggestNotification
from common.msg_bus import message_bus, Event
from common.loggers import app_log
from common.distributed_locks import distributed_lock


class ProductSuggests(object):
    suggest_notification = SuggestNotification()
    scheduler = BackgroundScheduler()

    def __init__(self):
        message_bus.add_event_listener(Event.TYPE_CONFIG_UPDATE, self.on_config_update_event)

    def start(self):
        """
        启动产品建议任务
        """
        app_log.info('Product Suggests begin starting')
        suggest_river_list = config.get_value('suggest/rivers/')
        index = 0
        for suggest_river in suggest_river_list:
            try:
                notification_config = get_dict_value_by_path('notification', suggest_river)
                trigger_time = notification_config.get('crontab')
                trigger = CronTrigger(**trigger_time)
                if trigger:
                    app_log.info('Add crontab job : {0}', notification_config)
                    suggest_river_name = suggest_river['name'] if 'name' in suggest_river else str(time.time())
                    notfification_notify_fun = distributed_lock.lock(suggest_river_name)(
                        self.suggest_notification.notify)
                    self.scheduler.add_job(notfification_notify_fun, args=[notification_config, suggest_river],
                                           trigger=trigger, id=('suggest_river_' + str(index)))
                    # notification_result = self.suggest_notification.notify(notification_config)
            except Exception as e:
                app_log.error('Suggest notification has error, suggest river is {0}', e, suggest_river)
            index += 1
        self.scheduler.add_listener(apscheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self.scheduler.start()
        app_log.info('Product Suggests started')

    def stop(self):
        """
        停止产品建议任务
        """
        try:
            app_log.info('Product Suggests begin stopping')
            self.scheduler.remove_all_jobs()
            self.scheduler.remove_listener(apscheduler_listener)
            self.scheduler.shutdown()
            app_log.info('Product Suggests stopped')
        except Exception as e:
            app_log.exception(e)

    def on_config_update_event(self):
        """
        响应配置数据变更事件
        """
        self.stop()
        self.start()


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


product_suggest = ProductSuggests()

if __name__ == '__main__':
    product_suggest.start()

    while True:
        time.sleep(10)
