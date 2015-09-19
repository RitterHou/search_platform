# coding=utf-8

import redis
from redis import RedisError

from common.configs import config
from utils import get_client_id
from common.loggers import app_log
from search_platform.settings import SERVICE_BASE_CONFIG


__author__ = 'liuzhaoming'

# 锁过期时间配置文件路径
expire_time_path = '/consts/global/lock_expire_time'


class LockStoreFactory(object):
    @staticmethod
    def get_store():
        # 目前只支持Redis同步锁
        lock_store_type = 'redis'
        if lock_store_type == 'redis':
            if 'redis' not in LOCKSTORE_DICT:
                # LOCKSTORE_DICT['redis'] = RedisLockStore(config.get_value('consts/global/lock_store'))
                LOCKSTORE_DICT['redis'] = RedisLockStore(SERVICE_BASE_CONFIG.get('redis_lock_store'))
        __lock_store = LOCKSTORE_DICT.get(lock_store_type)
        if not __lock_store:
            app_log.error('lock_store_type is invalid, {0}', None, lock_store_type)
        return __lock_store


class LockStore(object):
    """
    锁仓库
    """

    def get_lock_info(self, task_name, timeout=0):
        pass

    def release_lock_info(self, task_name):
        pass


class RedisLockStore(LockStore):
    """
    用Redis实现的锁仓库
    """

    def __init__(self, host):
        self.__has_initialized = False
        self.__host = host
        self.__init_store()

    def get_lock_info(self, task_name, timeout=0):
        """
        获取分布式锁
        :param task_name:
        :return:
        """
        task_key = self.__get_task_key(task_name)
        if not self.__redis_conn.setnx(task_key, self.__client_id):
            # 为了保证原子操作，首先对task中得锁进行认领，如果已经被占用，则认领不成功
            return False
        task_timeout = timeout or config.get_value(expire_time_path)
        self.__redis_conn.expire(task_key, task_timeout)
        return True

    def get_lock_info_by_watch(self, task_name, timeout=0):
        """
        通过watch实现分布式锁
        :param task_name:
        :return:
        """
        task_key = self.__get_task_key(task_name)
        pipe_line = self.__redis_conn.pipeline()
        try:
            pipe_line.watch(task_key)
            value = self.__redis_conn.get(task_key)
            if not value:
                timeout = timeout or config.get_value(expire_time_path)
                pipe_line.multi()
                pipe_line.setex(task_key, self.__client_id, timeout)
                pipe_line.execute()
                return True
            else:
                return False

        except RedisError as e:
            app_log.warning('get get_lock_info_by_watch has error {0}', e.message)
            return False
        finally:
            pipe_line.reset()


    def release_lock_info(self, task_name):
        task_key = self.__get_task_key(task_name)
        app_log.info('{0} relase lock {1}', self.__client_id, task_key)
        current_client_id = self.__redis_conn.get(task_key)
        if get_client_id() == current_client_id:
            # 此处不需要考虑事务，因为任务是有锁的，在delete之前不可能有其他的进程或线程获取到锁，本方法中redis get和delete一定是原子的
            self.__redis_conn.delete(task_key)
        else:
            app_log.info('cannot release the key, because the client id is others, {0}', current_client_id)

    def __get_task_key(self, task_name):
        """
        获取task对应的锁在redis中的key
        :param task_name:
        :return:
        """
        return ''.join(('distributed_lock_store', '|||', task_name))


    def __init_store(self):
        pool = redis.ConnectionPool.from_url(self.__host)
        self.__redis_conn = redis.Redis(connection_pool=pool)
        self.__has_initialized = True

        self.__client_id = get_client_id()


LOCKSTORE_DICT = {}


class DistributedLock(object):
    """
    分布式锁
    """

    def __init__(self):
        self.__lock_store = LockStoreFactory.get_store()

    def lock(self, task_name, time_out=0, is_release=True):
        """
        分布式锁
        """

        def decorator(function):

            def new_func(*args, **kwargs):
                lock_info = self.__lock_store.get_lock_info(task_name, timeout=time_out)
                if not lock_info:
                    app_log.info("{0} cannot get lock {1}", function, task_name)
                    return
                app_log.info("{0} get lock {1}", function, task_name)
                try:
                    result = function(*args, **kwargs)
                    app_log.info("{0} with lock {1}", function, task_name)
                except Exception as e:
                    raise e
                finally:
                    if is_release:
                        self.__lock_store.release_lock_info(task_name)

                return result

            return new_func

        return decorator


distributed_lock = DistributedLock()

if __name__ == '__main__':
    host = config.get_value('consts/global/lock_store') or 'redis://127.0.0.1:6379/1'
    lock_store = RedisLockStore(host)
    lock_store.get_conn().hmset('test_key', {{'houst1': {'key1': 'value1', 'key2': 122}, 'houst2': 'kkker'}})

