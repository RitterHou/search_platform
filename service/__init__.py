# coding=utf-8

__author__ = 'liuzhaoming'


def get_url(request):
    """
    获取请求的URL地址
    :param request:
    :return:
    """
    if not request:
        return ''
    return request._request.path


def desc_request(request):
    url = get_url(request)
    return '{0} {1} {2}'.format(url, request.QUERY_PARAMS, request.DATA)


def get_request_data(request):
    """
    获取HTTP请求数据，需要区分method处理
    :param request:
    :return:
    """
    if request.method == 'GET':
        return request.QUERY_PARAMS
    else:
        return request.POST

#
# def get_all_fields_from_request(request):
# if not request:
# return {}
# url = get_url(request)
