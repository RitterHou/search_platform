# -*- coding: utf-8 -*-
from rest_framework import status

from search_platform.responses import ExceptionResponse

# Create your views here.
from rest_framework.views import APIView
from rest_framework.response import Response

from service.req_router import request_router


__author__ = 'liuzhaoming'


class RestfulFacadeView(APIView):
    def get(self, request, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        return self.__handle_request(request, format)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        return self.__handle_request(request, format)

    def put(self, request, format=None):
        """
        PUT请求
        :param request:
        :param format:
        :return:
        """
        return self.__handle_request(request, format)

    def delete(self, request, format=None):
        """
        DELETE请求
        :param request:
        :param format:
        :return:
        """
        return self.__handle_request(request, format)

    def __handle_request(self, request, format):
        try:
            req_handler = request_router.route(request)
            if req_handler:
                result = req_handler.handle(request, format)
                return result if isinstance(result, Response) else Response(result)
            return Response(status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return ExceptionResponse(e)






