# -*- coding: utf-8 -*-
"""
Django settings for search_platform project.

For more information on this file, see
https://docs.djangoproject.com/en/1.7/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.7/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import sys

import os


reload(sys)
sys.setdefaultencoding('utf-8')
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.7/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '5l%66=pa_-pgs&9@w2fjaa(on638l6izt#-lx(2$(+roazdhbc'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

TEMPLATE_DEBUG = False

# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'common',
    'service',
    'river',
    'suggest',
    'manage',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'search_platform.urls'

WSGI_APPLICATION = 'search_platform.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.7/ref/settings/#databases

# DATABASES = {
# 'default': {
# 'ENGINE': 'django.db.backends.sqlite3',
# 'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
# }
# }

# Internationalization
# https://docs.djangoproject.com/en/1.7/topics/i18n/

LANGUAGE_CODE = 'zh-hans'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.7/howto/static-files/

STATIC_ROOT = ''
STATIC_URL = '/static/'
STATICFILES_DIRS = (
    os.path.join(os.path.dirname(__file__), '../static/').replace('\\', '/'),
)

TEMPLATE_DIRS = (
    os.path.join(BASE_DIR, 'templates'),
    os.path.join(BASE_DIR, 'templates/manage'),
)

ERROR_LOG_NUM = 3
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        }
    },
    'filters': {
        'sms_alarm_filter': {
            '()': 'redislog.tdummy.filters.QmAlarmFilter',
            'host': '192.168.65.183:2181,192.168.65.184:2181,192.168.65.185:2181',
            'phone_numbers': '15051885330,18651618480',
            'error_log_num': ERROR_LOG_NUM,
            'error_log_repr_list': ['elasticsearch.exceptions.ConnectionError',
                                    'elasticsearch.exceptions.ConnectionTimeout', 'DubboError', 'MsgQueueFullError',
                                    'RedoMsgQueueFullError', 'FinalFailMsgQueueFullError'
            ]
        }
    },
    'handlers': {
        'django_logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'django.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 40,
            'formatter': 'verbose',
        },
        'logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'root.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 40,
            'formatter': 'verbose',
        },
        'listener_logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'listener.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 20,
            'formatter': 'verbose',
        },
        'debug_logfile': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'debug.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 20,
            'formatter': 'verbose',
        },
        'interface_logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'interface.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 20,
            'formatter': 'verbose',
        },
        'error_logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'error.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 20,
            'formatter': 'verbose',
        },
        'query_logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(__file__), '../logs', 'query.log'),
            'maxBytes': 1024 * 1024 * 10,
            'backupCount': 20,
            'formatter': 'verbose',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        }
        # 'app_logstash_log': {
        #     'level': 'INFO',
        #     'class': 'redislog.tdummy.handlers.LogstashRedisHandler',
        #     'host': '192.168.65.224',
        #     'key': 'LOGSTASH_APP_LOG',
        #     'file_name': os.path.join(os.path.dirname(__file__), '../logs', 'logstatsh-redis-app.log'),
        #     'is_interface_handler': False
        # },
        # 'interface_logstash_log': {
        #     'level': 'INFO',
        #     'class': 'redislog.tdummy.handlers.LogstashRedisHandler',
        #     'host': '192.168.65.224',
        #     'key': 'LOGSTASH_INTF_LOG',
        #     'file_name': os.path.join(os.path.dirname(__file__), '../logs', 'logstatsh-redis-inf.log')
        # },
    },
    'loggers': {
        # 'app': {
        #     'handlers': ['logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'error': {
        #     'handlers': ['error_logfile'],
        #     'propagate': True,
        #     'level': 'ERROR',
        # },
        # 'interface': {
        #     'handlers': ['interface_logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'listener': {
        #     'handlers': ['listener_logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'debug': {
        #     'handlers': ['debug_logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'query': {
        #     'handlers': ['query_logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'django': {
        #     'handlers': ['django_logfile'],
        #     'propagate': True,
        #     'level': 'DEBUG',
        # },
        # 'django.request': {
        #     'handlers': ['django_logfile'],
        #     'level': 'DEBUG',
        #     'propagate': False,
        # },

        ####################################################
        'app': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'error': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
            'filters': ['sms_alarm_filter']
        },
        'interface': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'listener': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'debug': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'query': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'django': {
            'handlers': ['logfile'],
            'propagate': False,
            'level': 'INFO',
        },
        'django.request': {
            'handlers': ['logfile'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

ALLOWED_HOSTS = ['*']

REST_FRAMEWORK = {
    'DEFAULT_PARSER_CLASSES': (
        'rest_framework.parsers.JSONParser',
        'rest_framework.parsers.FormParser',
        'rest_framework.parsers.MultiPartParser',
        'rest_framework_xml.parsers.XMLParser',
        'rest_framework_yaml.parsers.YAMLParser',
    ),
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'search_platform.renderers.SearchBrowsableAPIRenderer',
        'rest_framework_xml.renderers.XMLRenderer',
        'rest_framework_yaml.renderers.YAMLRenderer',
        'rest_framework_csv.renderers.CSVRenderer',
        # 'rest_framework.renderers.TemplateHTMLRenderer',
    ),
    'EXCEPTION_HANDLER': 'search_platform.responses.custom_exception_handler'
}

SERVICE_BASE_CONFIG = {
    'meta_file': '/config',
    'redis': 'redis://172.17.8.253:6379/2',
    'elasticsearch': 'http://192.168.65.167:9200,http://192.168.65.175:9200,http://192.168.65.231:9200,http://192.168.65.241:9200,http://192.168.65.242:9200,http://192.168.65.245:9200',
    'meta_es_index': 'sp_search_platform_cfg',
    'meta_es_type': 'config',
    'meta_es_id': 'config_data',
    'message_bus_channel': 'search_platform_message_bus_channel',
    'celery_broker_url': 'redis://172.17.8.253:6379/0',
    'register_center_key': 'SEARCH_PLATFORM_REGISTER_CENTER',
    'msg_queue': 'redis://172.17.8.253:6379/4',
    'redis_admin_id_config': 'redis://172.17.8.253:6379/3',
    'register_zk_host': '192.168.65.183:2181,192.168.65.184:2181,192.168.65.185:2181'
}


# 测量结果ES别名
MEASUERE_ALIAS = 'sp_measure-alias'

VERSION = '1.3.0'

# from river import rivers