# coding=utf-8
from elasticsearch import TransportError
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import exception_handler

from common.exceptions import SearchPlatformException, ERROR_INFO, InvalidParamError, PKIsNullError
from common.loggers import query_log


__author__ = 'liuzhaoming'


class ExceptionResponse(Response):
    """
    处理返回异常的HTTP Response
    """

    def __init__(self, error, data=None, resp_status=None, template_name=None, headers=None, exception=False,
                 content_type=None):
        if isinstance(error, (InvalidParamError, PKIsNullError)):
            resp_status = resp_status or status.HTTP_400_BAD_REQUEST
            code = error.error_code
            message = error.error
        elif isinstance(error, SearchPlatformException):
            code = error.error_code
            message = error.error
            resp_status = resp_status or status.HTTP_500_INTERNAL_SERVER_ERROR
        elif isinstance(error, TransportError):
            code = ERROR_INFO['GenericError']['code']
            message = error.error
            resp_status = error.status_code if 'N/A' != error.status_code and error.status_code \
                else status.HTTP_500_INTERNAL_SERVER_ERROR
        else:
            code = ERROR_INFO['GenericError']['code']
            message = ' , '.join((str(error.__class__), str(error)))
            resp_status = resp_status or status.HTTP_500_INTERNAL_SERVER_ERROR
        if not data:
            data = {'status_code': resp_status, 'detail': message, 'error_code': code}
        Response.__init__(self, data, status=resp_status, template_name=template_name, headers=headers,
                          exception=exception,
                          content_type=content_type)


def custom_exception_handler(exc, context):
    response = exception_handler(exc, context)

    # Now add the HTTP status code to the response.
    if response is not None:
        response.data['status_code'] = response.status_code
    if exc:
        # 出现异常
        exception_response = ExceptionResponse(exc, resp_status=response.status_code if response else None)
        query_log.error('Http handler has error ', exc)
        return exception_response

    return response