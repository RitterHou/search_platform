# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'
import logging

from django.conf.urls import include
from django.contrib import admin
from django.conf.urls import url

from service import views
from common.configs import config


logger = logging.getLogger('root')


class URLFormat(object):
    def get_url_format_list(self):
        if 'url_pattern_list' in self.__dict__:
            return self.url_pattern_list
        else:
            chain = config.get_value('query/chain')
            if not chain:
                logger.info("chain is null")
                return []

            # url_format_list = [item['url_format'] for item in chain if 'url_format' in item and item['url_format']]
            # _url_pattern_list = map(lambda _url: url(_url, views.FacadeView.as_view()), url_format_list)
            _url_pattern_list = [url('[\\d\\D]+', views.FacadeView.as_view())]
            logger.info('_url_pattern_list = {0}'.format(_url_pattern_list))
            self.url_pattern_list = [url(r'^admin/', include(admin.site.urls)),
                                     url(r'^manage/', include('manage.urls'))] + _url_pattern_list
            return self.url_pattern_list


url_format = URLFormat()
