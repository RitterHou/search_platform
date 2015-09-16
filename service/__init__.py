# coding=utf-8
from common.utils import format_dict

__author__ = 'liuzhaoming'


def get_url(request, is_full=False):
    """
    获取请求的URL地址
    :param request:
    :return:
    """
    if not request:
        return ''
    return request.get_full_path() if is_full else request._request.path


def get_client_ip(request):
    if not request:
        return ''
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[-1].strip()
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def desc_request(request):
    url = get_url(request, True)
    client_ip = get_client_ip(request)
    return '{4} url={0}, client_ip={1}, params={2}, data={3}'.format(url, client_ip,
                                                                     format_dict(request.QUERY_PARAMS.lists()),
                                                                     format_dict(request.DATA.lists()), request.method)


def get_request_data(request):
    """
    获取HTTP请求数据，需要区分method处理
    :param request:
    :return:
    """
    if request.method == 'GET' or request.method == 'DELETE':
        return request.QUERY_PARAMS
    else:
        return request.DATA
