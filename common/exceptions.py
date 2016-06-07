# coding=utf-8
import time
__author__ = 'liuzhaoming'

ERROR_INFO = {'GenericError': {'code': 900, 'message': 'Generic exception'},
              'InvalidParamError': {'code': 1000, 'message': 'Param is invalid'},
              'UpdateDataNotExistError': {'code': 1001, 'message': 'The update data not exists'},
              'PKIsNullError': {'code': 1002, 'message': 'The resource id cannot be null'},
              'EsConnectionError': {'code': 1003, 'message': 'Cannot connect Es server'},
              'MsgHandlingFailError': {'code': 2000, 'message': 'MQ message handle error'},
              'MsgQueueFullError': {'code': 2001, 'message': 'MQ message queue is full'},
              'RedoMsgQueueFullError': {'code': 2002, 'message': 'Redo message queue is full'},
              'FinalFailMsgQueueFullError': {'code': 2003, 'message': 'Final message queue is full'},
              'EsBulkOperationError': {'code': 1004, 'message': 'Elasticsearch bulk operation fail'}}


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


class EsBulkOperationError(SearchPlatformException):
    """
    ES bulk操作异常
    """
    def __init__(self, bulk_result):
        self.bulk_result = bulk_result
        error_msg = ERROR_INFO['EsBulkOperationError']['message']
        SearchPlatformException.__init__(self, ERROR_INFO['EsBulkOperationError']['code'], error_msg)
class MsgQueueFullError(SearchPlatformException):
    """
    MQ队列已满
    """
    def __init__(self, admin_id, capacity, is_vip=True):
        SearchPlatformException.__init__(self, ERROR_INFO['MsgQueueFullError']['code'],
                                         ERROR_INFO['MsgQueueFullError']['message'])
        self._admin_id = admin_id
        self._capacity = capacity
        self._is_vip = is_vip
        self.is_threshold_alarm = True
    @property
    def admin_id(self):
        return self._admin_id
    @property
    def capacity(self):
        return self._capacity
    @property
    def is_vip(self):
        return self._is_vip
    def __str__(self):
        return '{0}(AdminID:{1}, Vip:{2}, Capacity:{3}, {4})'.format(str(self.__class__.__name__), self.admin_id,
                                                                     self.is_vip, self.capacity, self.error)
    def __unicode__(self):
        return '{0}(AdminID:{1}, Vip:{2}, Capacity:{3}, {4})'.format(str(self.__class__.__name__), self.admin_id,
                                                                     self.is_vip, self.capacity, self.error)
class RedoMsgQueueFullError(SearchPlatformException):
    """
    重做消息队列已满
    """
    def __init__(self, admin_id, capacity, is_vip=True):
        SearchPlatformException.__init__(self, ERROR_INFO['RedoMsgQueueFullError']['code'],
                                         ERROR_INFO['RedoMsgQueueFullError']['message'])
        self._admin_id = admin_id
        self._capacity = capacity
        self._is_vip = is_vip
        self.is_threshold_alarm = True
    @property
    def admin_id(self):
        return self._admin_id
    @property
    def capacity(self):
        return self._capacity
    @property
    def is_vip(self):
        return self._is_vip
    def __str__(self):
        return '{0}(AdminID:{1}, Vip:{2}, Capacity:{3}, {4})'.format(str(self.__class__.__name__), self.admin_id,
                                                                     self.is_vip, self.capacity, self.error)
    def __unicode__(self):
        return '{0}(AdminID:{1}, Vip:{2}, Capacity:{3}, {4})'.format(str(self.__class__.__name__), self.admin_id,
                                                                     self.is_vip, self.capacity, self.error)
class FinalFailMsgQueueFullError(SearchPlatformException):
    """
    最终失败消息队列已满
    """
    def __init__(self, capacity):
        SearchPlatformException.__init__(self, ERROR_INFO['FinalFailMsgQueueFullError']['code'],
                                         ERROR_INFO['FinalFailMsgQueueFullError']['message'])
        self._capacity = capacity
        self.is_threshold_alarm = True
    @property
    def capacity(self):
        return self._capacity
    def __str__(self):
        return '{0}(Capacity:{1}, {2})'.format(str(self.__class__.__name__), self.capacity, self.error)
    def __unicode__(self):
        return '{0}(Capacity:{1}, {2})'.format(str(self.__class__.__name__), self.capacity, self.error)
class MsgHandlingFailError(SearchPlatformException):
    """
    MQ 消息处理失败异常
    """
    DUBBO_ERROR = 1
    HTTP_ERROR = 2
    ES_READ_TIMEOUT = 3
    ES_ERROR = 4
    PROCESS_ERROR = 5
    def __init__(self, source):
        SearchPlatformException.__init__(self, ERROR_INFO['MsgHandlingFailError']['code'],
                                         ERROR_INFO['MsgHandlingFailError']['message'])
        self._source = source
        self._event_time = time.time()
    @property
    def source(self):
        return self._source
    @property
    def event_time(self):
        return self._event_time
    @property
    def json(self):
        return {"source": self.source, "event_time": self.event_time}
    def __str__(self):
        return '{0}({1}, {2}, source={3}, event_time={4})'.format(str(self.__class__.__name__), self.error_code,
                                                                  self.error, self.source, self.event_time)
    def __unicode__(self):
        return '{0}({1}, {2}, source={3}, event_time={4})'.format(str(self.__class__.__name__), self.error_code,
                                                                  self.error, self.source, self.event_time)
if __name__ == '__main__':
    error = InvalidParamError()
    print error
    print GenericError().error_code