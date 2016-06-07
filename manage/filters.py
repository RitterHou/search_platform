# coding=utf-8
import collections
import inspect

from common.adapter import es_adapter
from common.exceptions import InvalidParamError
from common.loggers import query_log


__author__ = 'liuzhaoming'


class ValidateFilter(object):
    def __init__(self):
        self.validate_fun_list = []

    def validate(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if self.validate_fun_list:
            validate_methods = self.validate_fun_list
        else:
            methods = inspect.getmembers(self, lambda value: isinstance(value, collections.Callable))
            validate_methods = (function for (name, function) in methods if name.startswith('validate_'))
        for function in validate_methods:
            function(request, pk, data, http_method, **kwargs)


class EsTmplValidateFilter(ValidateFilter):
    def __init__(self):
        ValidateFilter.__init__(self)
        self.validate_fun_list = (self.validate_pk, self.validate_delete, self.validate_fields)

    def validate_pk(self, request, pk=None, data=None, http_method='GET', **kwargs):
        """
        判断主键
        :param request:
        :param pk:
        :param data:
        :param http_method:
        :param kwargs:
        :return:
        """
        if http_method == 'POST':
            tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
            if pk in tmpl_cfg:
                raise InvalidParamError('The template already exists')
        elif http_method == 'DELETE':
            tmpl_cfg = es_adapter.get_config('es_index_setting').get('es_index_setting')
            if pk not in tmpl_cfg:
                raise InvalidParamError("The template doesn't exist")

    def validate_delete(self, request, pk=None, data=None, http_method='GET', **kwargs):
        """
        判断是否模板是否允许删除
        :param request:
        :param pk:
        :param data:
        :param http_method:
        :param kwargs:
        :return:
        """
        if http_method != 'DELETE':
            return
        if pk in ('product', 'gonghuo_product', 'suggest'):
            raise InvalidParamError("The template is system template, cannot be deleted")

    def validate_fields(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method in ('POST', 'PUT'):
            if not data:
                raise InvalidParamError("The template cannot be null")
            if not data.get('name'):
                raise InvalidParamError("The template name cannot be null")
            if not data.get('index'):
                raise InvalidParamError("The template index cannot be null")
            if not data.get('type'):
                raise InvalidParamError("The template type cannot be null")

            if data.get('mapping'):
                if not isinstance(data.get('mapping'), dict):
                    query_log.error('The template mapping should be json string, {0}', data.get('mapping'))
                    raise InvalidParamError("The template mapping should be json string")


class DataRiverValidateFilter(ValidateFilter):
    def __init__(self):
        ValidateFilter.__init__(self)

    def validate_pk(self, request, pk=None, data=None, http_method='GET', **kwargs):
        """
        判断主键
        :param request:
        :param pk:
        :param data:
        :param http_method:
        :param kwargs:
        :return:
        """
        if http_method == 'POST':
            river_cfg = es_adapter.get_config('data_river').get('data_river')
            if pk in river_cfg:
                raise InvalidParamError('The data river already exists')
        elif http_method == 'DELETE':
            river_cfg = es_adapter.get_config('data_river').get('data_river')
            if pk not in river_cfg:
                raise InvalidParamError("The data river doesn't exist")

    def validate_fields(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method in ('POST', 'PUT'):
            if not data:
                raise InvalidParamError("The data river cannot be null")
            if not data.get('name'):
                raise InvalidParamError("The data river name cannot be null")

    def validate_notification_field(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method not in ('POST', 'PUT'):
            return
        notification = data.get('notification')
        if not notification:
            raise InvalidParamError("The notification cannot be null")
        if notification.get('type') != 'MQ':
            raise InvalidParamError("The notification type is not valid")
        if not notification.get('host'):
            raise InvalidParamError("The notification host is not valid")
        if not notification.get('host'):
            raise InvalidParamError("The notification host is not valid")


class SuggestValidateFilter(ValidateFilter):
    def __init__(self):
        ValidateFilter.__init__(self)

    def validate_fields(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method in ('POST', 'PUT'):
            if not data:
                raise InvalidParamError("The suggest cannot be null")
            if not data.get('word'):
                raise InvalidParamError("The word cannot be null")
        elif http_method == 'DELETE':
            if not data:
                raise InvalidParamError("The suggest cannot be null")
            if not data.get('word'):
                raise InvalidParamError("The word cannot be null")


class MessageValidateFilter(ValidateFilter):
    def __init__(self):
        ValidateFilter.__init__(self)

    def validate_fields(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method in ('POST', 'PUT'):
            if not data:
                raise InvalidParamError("The message cannot be null")
            if not data.get('type'):
                raise InvalidParamError("The message type cannot be null")


class AnsjTokensValidateFilter(ValidateFilter):
    def __init__(self):
        ValidateFilter.__init__(self)

    def validate_fields(self, request, pk=None, data=None, http_method='GET', **kwargs):
        if http_method in ('POST', 'PUT'):
            if not data:
                raise InvalidParamError("The message cannot be null")
            if data.get('type') not in ('user_define', 'ambiguity'):
                raise InvalidParamError("The ansj token type is invalid")
            if data.get('operator') not in ('add', 'delete'):
                raise InvalidParamError("The ansj token operator is invalid")
            if not data.get('text'):
                raise InvalidParamError("The ansj token text is invalid")


estmpl_validater = EsTmplValidateFilter()
suggest_validater = SuggestValidateFilter()
message_validater = MessageValidateFilter()
ansj_validater = AnsjTokensValidateFilter()

if __name__ == '__main__':
    estmpl_validater = EsTmplValidateFilter()
    estmpl_validater.validate(None)

