# -*- coding: utf-8 -*-

from django.conf.urls import patterns, url

import views

__author__ = 'liuzhaoming'

urlpatterns = patterns('',
                       url('^datarivers$', views.DataRiverView.as_view()),
                       url('^datarivers/(?P<pk>[\d\D]*)$', views.DataRiverView.as_view()),
                       url('^estmpls$', views.EsTmplView.as_view()),
                       url('^estmpls/(?P<pk>[\d\D]*)$', views.EsTmplView.as_view()),
                       url('^querychains$', views.QueryChainView.as_view()),
                       url('^querychains/(?P<pk>[\d\D]*)$', views.QueryChainView.as_view()),
                       url('^sysparam$', views.SystemParamView.as_view()),
                       url('^processes$', views.SupervisorView.as_view()),
                       url('^processes/operations/(?P<action>[\d\D]*)/host/(?P<host>[\d\D]*)/name/(?P<process>[\d\D]*)$',
                           views.SupervisorActionView.as_view()),
                       url('^processes/operations/(?P<action>[\d\D]*)/host/(?P<host>[\d\D]*)$',
                           views.SupervisorActionView.as_view()),
                       url('^processes/(?P<host>[\d\D]*)$', views.SupervisorView.as_view()),
                       url('^ansjtokens$', views.AnsjSegmentationView.as_view()),
                       url('^suggestterms/(?P<adminID>[\d\D]+)/operations/(?P<operation>[\d\D]+)',
                           views.SuggestView.as_view()),
                       url('^suggestterms/(?P<adminID>[\d\D]+)/(?P<word>[\d\D]+)', views.SuggestView.as_view()),
                       url('^suggestterms/(?P<adminID>[\d\D]+)', views.SuggestView.as_view()),
                       url('^messages$', views.MessageView.as_view()),
                       url(r'^process/log', 'manage.views.view_log'),
                       url('^shops/(?P<admin_id>[\d\D]+)/products$', views.ProductView.as_view()),
                       url('^shops/(?P<admin_id>[\d\D]+)/products/(?P<doc_id>[\d\D]+)$', views.ProductView.as_view()),
                       url('^shops/(?P<admin_id>[\d\D]+)/operations/(?P<operation>[\d\D]+)$', views.ShopView.as_view()),
                       url('^shops/(?P<admin_id>[\d\D]+)$', views.ShopView.as_view()),
                       url('^shops$', views.ShopView.as_view()),
                       url('^indexes/(?P<index>[\d\D]+)/(?P<type>[\d\D]+)/operations/(?P<operation>[\d\D]+)$', views.EsIndexView.as_view()),
                       url('^indexes/(?P<index>[\d\D]+)/(?P<type>[\d\D]+)/docs$', views.EsDocView.as_view()),
                       url('^indexes/(?P<index>[\d\D]+)/(?P<type>[\d\D]+)/docs/(?P<doc_id>[\d\D]+)$', views.EsDocView.as_view()),
                       url('^indexes/(?P<index>[\d\D]+)/(?P<type>[\d\D]+)$', views.EsIndexView.as_view()),
                       url('^indexes/(?P<index>[\d\D]+)$', views.EsIndexView.as_view()),
                       url('^indexes$', views.EsIndexView.as_view()),
                       url('^docs$', views.EsDocView.as_view()),
                       url(r'^$', 'manage.views.manage'),
)