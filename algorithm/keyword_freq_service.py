# coding=utf-8
"""
商品搜索关键词存放记录
"""

from Queue import Queue
from itertools import groupby, repeat
import threading
import time

from common.connections import RedisConnectionFactory

from common.loggers import app_log
from search_platform import settings


__author__ = 'liuzhaoming'

MAX_KEYWORD_LEN = 20
CARRY_PERIOD = 10
CARRY_MAX_NUM = 1000
REDIS_KEYWORD_KEY = 'ps_keywords_{0}'


class RedisKeywordFreqService(object):
    """
    使用Redis作为商品搜索关键词频率服务接口
    """

    def __init__(self):
        self._queue = Queue(10000)
        self._is_start = False
        self._carry_thread = None
        self._mutex = threading.Lock()
        self._redis_conn = None
        self._redis_host = settings.SERVICE_BASE_CONFIG.get('keywords_redis_host')

    def add(self, admin_id, keyword):
        """
        添加搜索关键词
        :param admin_id:
        :param keyword:
        :return:
        """
        if not admin_id or not keyword or len(keyword) > MAX_KEYWORD_LEN:
            return False

        try:
            if not self._is_start:
                if self._mutex.acquire():
                    if self._is_start:
                        self._mutex.release()
                    else:
                        self._start()
                        self._mutex.release()
            self._queue.put_nowait((admin_id, keyword))
        except Exception as e:
            app_log.error('RedisKeywordFreqService put keyword to queue fail {0} , {1}', e, admin_id, keyword)

    def get_keyword_freq(self, admin_id, keyword_list):
        """
        查询keyword的搜索次数
        :param admin_id:
        :param keyword_list:
        :return:
        """
        if not admin_id or not keyword_list:
            return []

        if not self._redis_conn:
            return list(repeat(0, len(keyword_list)))

        admin_keywords_key = REDIS_KEYWORD_KEY.format(admin_id)
        keyword_freq_list = self._redis_conn.hmget(admin_keywords_key, keyword_list)
        return map(lambda keyword_freq: int(keyword_freq) if keyword_freq else 0, keyword_freq_list)

    def _start(self):
        """
        开启将内存中的关键词搜索记录写入到Redis线程
        :return:
        """
        try:
            self._carry_thread = threading.Thread(target=self._time_handle_carry_to_redis, name='Carry keyword thread')
            self._carry_thread.setDaemon(True)
            self._redis_conn = RedisConnectionFactory.get_redis_connection(self._redis_host)
            self._carry_thread.start()
            self._is_start = True
        except Exception as e:
            app_log.error('RedisKeywordFreqService start put redis thread fail, ', e)

    def _time_handle_carry_to_redis(self):
        """
        周期执行关键词存放到redis
        :return:
        """
        while True:
            start_time = time.time()
            try:
                self._carry_to_redis()
            except:
                pass

            spend_time = time.time() - start_time
            if spend_time < CARRY_PERIOD:
                time.sleep(CARRY_PERIOD - spend_time)

    def _carry_to_redis(self):
        """
        将内存中的关键词搜索记录存放到Redis中
        :return:
        """
        record_list = []
        num = 0
        while True:
            try:
                admin_id, keyword = self._queue.get(block=False)
                record_list.append((admin_id, keyword))
                num += 1
                if num > CARRY_MAX_NUM:
                    break
            except:
                break

        if not record_list:
            return

        for admin_id, records in groupby(record_list, lambda item: item[0]):
            admin_keywords_key = REDIS_KEYWORD_KEY.format(admin_id)
            for record in records:
                try:
                    self._redis_conn.hincrby(admin_keywords_key, record[1])
                except Exception as e:
                    app_log.error('RedisKeywordFreqService put keyword to redis error {} {}', e, admin_id, keyword)


keyword_freq_service = RedisKeywordFreqService()

if __name__ == '__main__':
    keyword_freq_service.add('A000000', u'苹果手机')
    keyword_freq_service.add('A000000', u'苹果手机')
    keyword_freq_service.add('A000000', u'apple')

    while True:
        print keyword_freq_service.get_keyword_freq('A000000', [u'苹果手机', u'apple', u'huawei'])
        print '\n'
        print keyword_freq_service.get_keyword_freq('A000000', ['苹果手机', 'apple', 'huawei'])
        print '\n'
        print keyword_freq_service.get_keyword_freq('A0000001', [u'苹果手机', u'apple', u'huawei'])
        print '\n'
        time.sleep(10)