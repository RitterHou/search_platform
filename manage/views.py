# -*- coding: utf-8 -*-
# Create your views here.
from collections import OrderedDict
import datetime

from django.http import JsonResponse, QueryDict
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from common.configs import config
from common.exceptions import PKIsNullError, InvalidParamError
from common.utils import merge
from search_platform import settings
from search_platform.responses import ExceptionResponse
from manage.filters import estmpl_validater, suggest_validater, message_validater, ansj_validater
from models import supervisor, data_river, es_tmpl, query_chain, sys_param, message, ansjSegmentation, suggest, \
    es_index, shop, shop_product, es_doc


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
        if pk and (not data_list or len(data_list) == 0):
            # 如果指定了名称，根据REST规范，返回单个资源，如果该资源不存在，返回404
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(data_list)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        data_river.save(data)
        return Response(data)

    def put(self, request, pk=None, format=None):
        if not pk:
            return ExceptionResponse(PKIsNullError(), resp_status=status.HTTP_400_BAD_REQUEST)
        data = OrderedDict(request.DATA)
        data_river.update(data)
        return Response(data)

    def delete(self, request, pk=None, format=None):
        if not pk:
            return ExceptionResponse(PKIsNullError(), resp_status=status.HTTP_400_BAD_REQUEST)
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
        estmpl_validater.validate(request, pk, http_method='GET')
        data_list = es_tmpl.get(pk)
        if pk and (not data_list or len(data_list) == 0):
            # 如果指定了名称，根据REST规范，返回单个资源，如果该资源不存在，返回404
            return Response(data={'status_code': status.HTTP_404_NOT_FOUND, 'detail': 'Cannot find resource'},
                            status=status.HTTP_404_NOT_FOUND)
        return Response(data_list)

    def post(self, request, pk=None, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        if not data.get('host'):
            data['host'] = settings.SERVICE_BASE_CONFIG['elasticsearch']
        estmpl_validater.validate(request, pk, data, http_method='POST')
        es_tmpl.save(data)
        return Response(data)

    def put(self, request, pk=None, format=None):
        data = request.DATA
        if not data.get('host'):
            data['host'] = settings.SERVICE_BASE_CONFIG['elasticsearch']
        estmpl_validater.validate(request, pk, data, http_method='PUT')
        es_tmpl.update(data)
        return Response(data)

    def delete(self, request, pk=None, format=None):
        estmpl_validater.validate(request, pk, http_method='DELETE')
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
        if pk and (not data_list or len(data_list) == 0):
            # 如果指定了名称，根据REST规范，返回单个资源，如果该资源不存在，返回404
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(data_list)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        query_chain.save(data)
        return Response(data)

    def put(self, request, pk=None, format=None):
        if not pk:
            return ExceptionResponse(PKIsNullError(), resp_status=status.HTTP_400_BAD_REQUEST)
        data = OrderedDict(request.DATA)
        query_chain.update(data)
        return Response(data)

    def delete(self, request, pk, format=None):
        if not pk:
            return ExceptionResponse(PKIsNullError(), resp_status=status.HTTP_400_BAD_REQUEST)
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
        system_param_cfg = sys_param.get()
        return Response(system_param_cfg)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        sys_param.save(data)
        return Response(data)


class MessageView(APIView):
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
        data_list = message.get()
        return Response(data_list)

    def post(self, request, format=None):
        """
        POST请求
        :param request:
        :param format:
        :return:
        """
        data = OrderedDict(request.DATA)
        message_validater.validate(request, data=data, http_method='POST')
        if 'send_time' not in data:
            now = datetime.datetime.now()
            format_time = now.strftime("%Y-%m-%d %H:%M:%S")
            data['send_time'] = format_time
        if 'destination' not in data:
            data['destination'] = 'all'
        data['source'] = self.get_client_ip(request)
        message.send(data)
        return Response(data)

    @staticmethod
    def get_client_ip(request):
        """
        获取客户端IP地址
        """
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[-1].strip()
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip


class SupervisorView(APIView):
    """
    进程管理
    """

    def get(self, reqeust, host=None, format=None):
        supervisor_info_list = supervisor.get_cluster_supervisor_info(host)
        if host and (not supervisor_info_list or len(supervisor_info_list) == 0):
            # 如果指定了名称，根据REST规范，返回单个资源，如果该资源不存在，返回404
            return Response(data={'status_code': status.HTTP_404_NOT_FOUND, 'detail': 'Cannot find resource'},
                            status=status.HTTP_404_NOT_FOUND)
        return Response(supervisor_info_list)


class SupervisorActionView(APIView):
    """
    进程管理操作
    """

    def post(self, request, action, host=None, process=None, format=None):
        result = supervisor.do_action(host, action, process)
        return Response({})

    def get(self, request, action, host=None, process=None, format=None):
        if action != 'get_log':
            return Response({'detail': 'Only get_log action is allowed', 'status_code': status.HTTP_406_NOT_ACCEPTABLE},
                            status.HTTP_406_NOT_ACCEPTABLE)
        result = supervisor.do_action(host, action, process)
        return Response(result)


class AnsjSegmentationView(APIView):
    def post(self, request, format=None):
        data = OrderedDict(request.DATA)
        ansj_validater.validate(request, data=data, http_method='POST')
        # data['operate'] = 'add'
        ansjSegmentation.set_segmentation(data)
        return Response(data)


    def delete(self, request, format=None):
        data = OrderedDict(request.DATA)
        # data['operate'] = 'delete'
        ansjSegmentation.set_segmentation(data)
        return Response({})


class SuggestView(APIView):
    def post(self, request, adminID, operation=None, format=None):
        data = OrderedDict(request.DATA)
        if operation == 'init':
            # 表示需要对suggest 索引执行初始化操作
            suggest.init_suggest_index(adminID)
        else:
            suggest_validater.validate(request, adminID, data, 'POST')
            data['adminID'] = adminID
            if not data.get('source_type'):
                data['source_type'] = '2'
            suggest.add_suggest_term(data)
        return Response(data)

    def get(self, request, adminID, format=None):
        suggest_terms = suggest.query_suggest_terms(adminID)
        return Response(suggest_terms)


    def delete(self, request, adminID, word, format=None):
        data = OrderedDict(request.QUERY_PARAMS)
        data['word'] = word
        data['adminID'] = adminID
        if not data.get('source_type'):
            data['source_type'] = '1'
        suggest_validater.validate(request, adminID, data, 'DELETE')
        suggest.delete_suggest_term(data)
        return Response({})


class EsIndexView(APIView):
    def post(self, request, index=None, type=None, operation=None, format=None):
        data = self.__get_es_cfg(request.DATA, index, type)
        if not index:
            raise InvalidParamError('Index cannot be null')
        data['index'] = index
        if not type:
            raise InvalidParamError('Type cannot be null')
        data['type'] = type
        if operation == 'init':
            es_index.delete_type(data)
            es_index.add_index(data)
        elif operation:
            raise InvalidParamError('Cannot find resource {0}'.format(operation))
        else:
            es_index.add_index(data)

        return Response(data)

    def get(self, request, index=None, format=None):
        data = OrderedDict(request.QUERY_PARAMS)
        if index:
            data['index'] = index
        return Response(es_index.query_es_index_info_list(data))

    def delete(self, request, index=None, type=None, format=None):
        """
        删除操作
        """
        data = OrderedDict(request.QUERY_PARAMS)
        if not index:
            raise InvalidParamError('Index cannot be null')
        data['index'] = index
        if not type:
            es_index.delete_index(data)
            return Response()
        data['type'] = type
        es_index.delete_type(data)
        return Response()

    def __get_es_cfg(self, request_data, index, type):
        """
        获取ES配置
        """
        es_cfg = {}
        if isinstance(request_data, QueryDict):
            for (key, value) in request_data.iteritems():
                if isinstance(value, list) and len(value):
                    es_cfg[key] = value[0]
        else:
            es_cfg = dict(request_data)
        if es_cfg.get('reference'):
            es_setting = config.get_value('/es_index_setting/' + es_cfg.get('reference'))
            es_cfg = merge(es_setting, es_cfg)
        es_cfg.update({"index": index, "type": type})
        return es_cfg


class ShopView(APIView):
    def post(self, request, admin_id, operation=None, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_404_NOT_FOUND},
                            status.HTTP_404_NOT_FOUND)
        data = OrderedDict(request.DATA)
        if operation == 'init':
            shop.delete_shop(admin_id)
            shop.add_shop(admin_id)
        elif operation:
            return Response({'detail': 'The operaton is invalid', 'status_code': status.HTTP_406_NOT_ACCEPTABLE},
                            status.HTTP_406_NOT_ACCEPTABLE)
        else:
            shop.add_shop(admin_id)
        return Response(data)

    def get(self, request, admin_id=None, format=None):
        return Response(shop.query_shops(admin_id))

    def delete(self, request, admin_id, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_404_NOT_FOUND},
                            status.HTTP_404_NOT_FOUND)
        shop.delete_shop(admin_id)
        return Response()


class ProductView(APIView):
    def get(self, request, admin_id, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        return Response(shop_product.query(request.QUERY_PARAMS, admin_id))

    def delete(self, request, admin_id, doc_id, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        if not doc_id:
            shop_product.delete(request.QUERY_PARAMS, admin_id)
        else:
            shop_product.delete_by_id(request.QUERY_PARAMS, admin_id, doc_id)
        return Response()

    def post(self, request, admin_id, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        if not request.DATA.get('data'):
            return Response({'detail': 'Product cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        shop_product.add(request.DATA.get('data'), admin_id)
        return Response()

    def put(self, request, admin_id, format=None):
        if not admin_id:
            return Response({'detail': 'Admin ID cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        if not request.DATA.get('data'):
            return Response({'detail': 'Product cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        shop_product.update(request.DATA.get('data'), admin_id)
        return Response()


class EsDocView(APIView):
    def get(self, request, index, type, format=None):
        es_cfg = self.__get_es_cfg(request.QUERY_PARAMS, index, type)
        return Response(es_doc.query(es_cfg, request.QUERY_PARAMS))

    def post(self, request, index, type, format=None):
        es_cfg = self.__get_es_cfg(request.DATA, index, type)
        self.__validate_es_cfg_id(es_cfg, 'id')
        data = request.DATA.get('data')
        if not data:
            return Response({'detail': 'The docs data cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        elif not isinstance(data, (list, tuple, set)):
            data = [data]

        es_doc.add(es_cfg, data)
        return Response(data)

    def put(self, request, index, type, format=None):
        es_cfg = self.__get_es_cfg(request.DATA, index, type)
        self.__validate_es_cfg_id(es_cfg, 'id')
        data = request.DATA.get('data')
        if not data:
            return Response({'detail': 'The docs data cannot be null', 'status_code': status.HTTP_400_BAD_REQUEST},
                            status.HTTP_400_BAD_REQUEST)
        elif not isinstance(data, (list, tuple, set)):
            data = [data]

        es_doc.update(es_cfg, data)
        return Response(data)

    def delete(self, request, index, type, doc_id=None, format=None):
        es_cfg = self.__get_es_cfg(request.QUERY_PARAMS, index, type)
        if not doc_id:
            # 根据查询条件删除文档
            es_doc.delete_by_query(es_cfg, request.QUERY_PARAMS)
        else:
            es_doc.delete_by_id(es_cfg, doc_id)

        return Response()

    def __get_es_cfg(self, request_data, index, type):
        """
        获取ES配置
        """
        es_cfg = {}
        if isinstance(request_data, QueryDict):
            for (key, value) in request_data.iteritems():
                if isinstance(value, list) and len(value):
                    es_cfg[key] = value[0]
        else:
            es_cfg = dict(request_data)
        if es_cfg.get('reference'):
            es_setting = config.get_value('/es_index_setting/' + es_cfg.get('reference'))
            es_cfg = merge(es_setting, es_cfg)
        es_cfg.update({"index": index, "type": type})
        return es_cfg

    def __validate_es_cfg_id(self, es_cfg, key):
        """
        检查ES参数是否合法
        """
        if not es_cfg.get(key):
            raise InvalidParamError('The doc {0} cannot be null'.format(key))


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
    return render(request, 'management.html', {'version': config.get_value('version')})