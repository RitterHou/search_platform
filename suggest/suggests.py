# -*- coding: utf-8 -*-
import json
import urllib2

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
from search_platform import settings
from manage.models import yxd_shop_suggest, suggest


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
                if not trigger_time:
                    continue
                trigger = CronTrigger(**trigger_time)
                if trigger:
                    app_log.info('Add crontab job : {0}', notification_config)
                    suggest_river_name = suggest_river['name'] if 'name' in suggest_river else str(time.time())
                    notification_notify_fun = distributed_lock.lock(suggest_river_name)(
                        self.suggest_notification.notify)
                    self.scheduler.add_job(notification_notify_fun, args=[notification_config, suggest_river],
                                           trigger=trigger, id=('suggest_river_' + str(index)))
                    notification_result = self.suggest_notification.notify(notification_config, suggest_river)
                # 云小店商品提示数据定时处理任务
                app_log.info('Add yxd crontab job...')
                yxd_suggest_task_func = distributed_lock.lock('yxd_suggest_task_lock')(yxd_suggest_task)
                self.scheduler.add_job(yxd_suggest_task_func, 'cron', day_of_week='mon', hour=3)
            except Exception as e:
                app_log.error('Suggest notification has error, suggest river is {0}', e, suggest_river)
            index += 1
        self.scheduler.add_listener(apscheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        try:
            self.scheduler.start()
        except Exception as e:
            app_log.error("Product Suggests start has error ", e)
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


def yxd_suggest_task():
    """
    云小店根据用户A编号定时的处理并保存商品提示数据
    :return:
    """
    app_log.info('yxd user\'s goods and shop data sync task started.')
    search_platform_host = settings.SERVICE_BASE_CONFIG['search_platform_host']
    search_url = search_platform_host + '/usercenter/shops?ex_q_sceneBname=terms(cloudShop)&ex_q_signStatus=terms(2)'
    response = urllib2.urlopen(urllib2.Request(search_url)).read()
    result = json.loads(response)

    store_names = []
    for admin in result['root']:
        admin_id = admin['adminId']
        suggest.init_suggest_index(admin_id)
        store_names.append(admin['storeName'])
    yxd_shop_suggest.init_suggest(store_names)


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
