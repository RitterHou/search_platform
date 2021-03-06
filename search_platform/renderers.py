# coding=utf-8
from rest_framework.renderers import BrowsableAPIRenderer, JSONRenderer

from search_platform import settings


__author__ = 'liuzhaoming'


class SearchBrowsableAPIRenderer(BrowsableAPIRenderer):
    template = 'service/api.html'

    def get_default_renderer(self, view):
        return JSONRenderer()

    def get_context(self, data, accepted_media_type, renderer_context):
        context = super(SearchBrowsableAPIRenderer, self).get_context(data, accepted_media_type, renderer_context)
        context['version'] = settings.VERSION
        return context