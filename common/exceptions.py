# coding=utf-8
__author__ = 'liuzhaoming'

ERROR_INFO = {'GenericError': {'code': 900, 'message': 'eneric exception'},
              'InvalidParamError': {'code': 1000, 'message': 'Param is invalid'},
              'UpdateDataNotExistError': {'code': 1001, 'message': 'The update data not exists'},
              'PKIsNullError': {'code': 1002, 'message': 'The resource id cannot be null'},
              'EsConnectionError': {'code': 1003, 'message': 'Cannot connect Es server'}, }


class SearchPlatformException(Exception):
    """
    搜索平台自定义异常
    """

    @property
    def error_code(self):
        return self.args[0]

    @property
    def error(self):
        return self.args[1]

    def __str__(self):
        return '{0}({1}, {2})'.format(str(self.__class__.__name__), self.error_code, self.error)

    def __unicode__(self):
        return '{0}({1}, {2})'.format(str(self.__class__.__name__), self.error_code, self.error)


class GenericError(SearchPlatformException):
    """
    通用异常
    """

    def __init__(self, msg):
        if msg:
            error_msg = '. '.join((ERROR_INFO['GenericError']['message'], msg))
        else:
            error_msg = ERROR_INFO['GenericError']['message']
        SearchPlatformException.__init__(self, ERROR_INFO['GenericError']['code'], error_msg)


class InvalidParamError(SearchPlatformException):
    """
    参数不合法
    """

    def __init__(self, msg):
        if msg:
            error_msg = '. '.join((ERROR_INFO['InvalidParamError']['message'], msg))
        else:
            error_msg = ERROR_INFO['InvalidParamError']['message']
        SearchPlatformException.__init__(self, ERROR_INFO['InvalidParamError']['code'], error_msg)


class UpdateDataNotExistError(SearchPlatformException):
    """
    要更新的数据不存在
    """

    def __init__(self, msg):
        if msg:
            error_msg = '. '.join((ERROR_INFO['UpdateDataNotExistError']['message'], msg))
        else:
            error_msg = ERROR_INFO['UpdateDataNotExistError']['message']
        SearchPlatformException.__init__(self, ERROR_INFO['UpdateDataNotExistError']['code'], error_msg)


class PKIsNullError(SearchPlatformException):
    """
    主键不能为空
    """

    def __init__(self):
        SearchPlatformException.__init__(self, ERROR_INFO['PKIsNullError']['code'],
                                         ERROR_INFO['PKIsNullError']['message'])


class EsConnectionError(SearchPlatformException):
    """
    连接不上ES服务器
    """

    def __init__(self):
        SearchPlatformException.__init__(self, ERROR_INFO['EsConnectionError']['code'],
                                         ERROR_INFO['EsConnectionError']['message'])


if __name__ == '__main__':
    error = InvalidParamError()
    print error
    print GenericError().error_code