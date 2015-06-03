# coding=utf-8
import thread

from common.distributed_locks import distributed_lock


__author__ = 'liuzhaoming'


@distributed_lock.lock("test.run")
def run(name, i):
    print '{0} iter {1}'.format(name, i)


def iter_run(name, times):
    import time

    for i in xrange(times):
        run(name, i)
        time.sleep(2)


if __name__ == '__main__':
    for i in xrange(2):
        thread.start_new_thread(iter_run, ('name' + str(i), i + 1))

    print "finish"

