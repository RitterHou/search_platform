# -*- coding: utf-8 -*-
"""
对由Supervisor启动的子进程进行监控，一旦进程退出则发出提醒
"""
import sys
import urllib2
import urllib
import json
import datetime
import socket


def write_stdout(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def str_2_json(string):
    return dict([x.split(':') for x in string.split()])


def get_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(('8.8.8.8', 80))
        ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ip


def main():
    while 1:
        write_stdout('READY\n')

        line = sys.stdin.readline()
        header = str_2_json(line)
        line = sys.stdin.read(int(header['len']))
        body = str_2_json(line)

        body.update(header)
        body['time'] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        body['ip'] = get_ip()

        data = json.dumps(body)

        url = 'http://gavin.common.qianmi.com/weixin/sendToUser?jobNo=of2603&message={}'.format(urllib.quote(data))
        urllib2.urlopen(urllib2.Request(url)).read()

        write_stdout('RESULT 2\nOK')


if __name__ == '__main__':
    main()
