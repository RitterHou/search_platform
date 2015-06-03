# -*- coding: utf-8 -*-
# Create your views here.
from collections import OrderedDict

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from models import supervisor, data_river, es_tmpl, query_chain, sys_param


class DataRiverView(APIView):
    """
    数据流
    """

    def get(self, request, pk=None, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        data_list = data_river.get(pk)
        return Response(data_list)

    def post(self, request, pk=None, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        data_river.save(data)
        return Response({})

    def put(self, request, pk=None, format=None):
        data = OrderedDict(request.DATA)
        data_river.update(data)
        return Response({})

    def delete(self, request, pk=None, format=None):
        data_river.delete(pk)
        return Response({})


class EsTmplView(APIView):
    """
    ES模板
    """

    def get(self, request, pk=None, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        data_list = es_tmpl.get(pk)
        return Response(data_list)

    def post(self, request, pk=None, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        es_tmpl.save(data)
        return Response({})

    def put(self, request, pk=None, format=None):
        data = request.DATA
        es_tmpl.update(data)
        return Response({})

    def delete(self, request, pk, format=None):
        es_tmpl.delete(pk)
        return Response({})


class QueryChainView(APIView):
    """
    REST处理器
    """

    def get(self, request, pk=None, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        data_list = query_chain.get(pk)
        return Response(data_list)

    def post(self, request, pk=None, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        query_chain.save(data)
        return Response({})

    def put(self, request, pk=None, format=None):
        data = OrderedDict(request.DATA)
        query_chain.update(data)
        return Response({})

    def delete(self, request, pk, format=None):
        query_chain.delete(pk)
        return Response({})


class SystemParamView(APIView):
    """
    系统参数
    """

    def get(self, request, format=None):
        """
        get请求
        :param request:
        :param format:
        :return:
        """
        data_list = sys_param.get()
        return Response(data_list)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        sys_param.save(data)
        return Response({})


class SupervisorView(APIView):
    """
    进程管理
    """

    def get(self, reqeust, host=None, format=None):
        supervisor_info_list = supervisor.get_cluster_supervisor_info(host)
        return Response(supervisor_info_list)


class SupervisorActionView(APIView):
    """
    进程管理操作
    """

    def post(self, request, action, host=None, process=None, format=None):
        result = supervisor.do_action(host, action, process)
        return Response({'result': result or 'success'})

    def get(self, request, action, host=None, process=None, format=None):
        if action is not 'get_log':
            return Response(status.HTTP_406_NOT_ACCEPTABLE)
        result = supervisor.do_action(host, action, process)
        return Response(result)


def supervisor_index(request):
    """
    进程管理主页
    :param request:
    :return:
    """
    supervisor_info_list = supervisor.get_cluster_supervisor_info()
    return render(request, 'supervisor.html', {'hosts': supervisor_info_list})


@csrf_exempt
def get_supervisor_info(request):
    """
    获取搜索平台集群信息
    :param request:
    :return:
    """
    host = request.POST.get("host")
    supervisor_info_list = supervisor.get_cluster_supervisor_info(host)
    return JsonResponse(supervisor_info_list)


@csrf_exempt
def do_supervisor_action(request):
    """
    执行supervisor操作
    :param request:
    :return:
    """
    host = request.POST.get("host")
    action = request.POST.get("action")
    process = request.POST.get("process")
    result = supervisor.do_action(host, action, process)
    return JsonResponse({'result': result or 'success'})


def view_log(request):
    host = request.GET.get("host")
    process = request.GET.get("process")
    result = supervisor.do_action(host, 'get_log', process)
    log_rows = result.split('\n')
    return render(request, 'view_log.html', {'result': log_rows})


def manage(request):
    return render(request, 'manage.html')