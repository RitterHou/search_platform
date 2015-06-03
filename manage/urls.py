# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'
from django.conf.urls import patterns, url

import views


urlpatterns = patterns('',
                       url(r'^process/$', 'manage.views.supervisor_index'),
                       url(r'^process/action.do$', 'manage.views.do_supervisor_action'),
                       url(r'^process/viewlog$', 'manage.views.view_log'),
                       url('^datarivers$', views.DataRiverView.as_view()),
                       url('^datarivers/(?P<pk>[\d\D]*)$', views.DataRiverView.as_view()),
                       url('^estmpls$', views.EsTmplView.as_view()),
                       url('^estmpls/(?P<pk>[\d\D]*)$', views.EsTmplView.as_view()),
                       url('^querychains$', views.QueryChainView.as_view()),
                       url('^querychains/(?P<pk>[\d\D]*)$', views.QueryChainView.as_view()),
                       url('^sysparams$', views.SystemParamView.as_view()),
                       url('^processes$', views.SupervisorView.as_view()),
                       url('^processes/action/(?P<action>[\d\D]*)/host/(?P<host>[\d\D]*)/name$',
                           views.SupervisorActionView.as_view()),
                       url('^processes/action/(?P<action>[\d\D]*)/host/(?P<host>[\d\D]*)/name/(?P<process>[\d\D]*)$',
                           views.SupervisorActionView.as_view()),
                       url('^processes/(?P<host>[\d\D]*)$', views.SupervisorView.as_view()),
                       url(r'^$', 'manage.views.manage'),
)