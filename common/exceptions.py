# coding=utf-8
__author__ = 'liuzhaoming'


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


class InvalidParamError(SearchPlatformException):
    """
    参数不合法
    """

    def __init__(self):
        SearchPlatformException.__init__(self, 1000, 'Param is invalid')

class UpdateDataNotExistError(SearchPlatformException):
    """
    要更新的数据不存在
    """

    def __init__(self):
        SearchPlatformException.__init__(self, 1000, 'The update data not exists')


class A(object):
    def test(self):
        print str(self.__class__.__name__)


class B(A):
    pass


if __name__ == '__main__':
    a = A()
    print str(a.__class__)
    a.test()
    b = B()
    b.test()
    error = InvalidParamError()
    print error