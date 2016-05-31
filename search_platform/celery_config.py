# -*- coding: utf-8 -*-
from __future__ import absolute_import


import os
from celery import Celery
from django.conf import settings

__author__ = 'liuzhaoming'
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'search_platform.settings')

broker_url = settings.SERVICE_BASE_CONFIG['celery_broker_url']
backend_url = settings.SERVICE_BASE_CONFIG['celery_broker_url']
app = Celery('tasks', broker=broker_url)  # , backend=backend_url)
# app = Celery('tasks', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
