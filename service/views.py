# -*- coding: utf-8 -*-
from rest_framework import status

__author__ = 'liuzhaoming'

# Create your views here.
from rest_framework.views import APIView
from rest_framework.response import Response

from service.req_router import request_router


class FacadeView(APIView):
    """
    搜索平台
    """

    def get(self, request, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        req_handler = request_router.route(request)
        if req_handler:
            return Response(req_handler.handle(request, format))
        return Response(status=status.HTTP_404_NOT_FOUND)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        req_handler = request_router.route(request)
        if req_handler:
            return Response(req_handler.handle(request, format))
        return Response(status=status.HTTP_404_NOT_FOUND)

    def put(self, request, format=None):
        pass

    def delete(self, request, format=None):
        pass






