# -*- coding: utf-8 -*-
__author__ = 'liuzhaoming'

import SocketServer

# This class defines response to each request
class MyTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # self.request is the TCP socket connected to the client
        request = self.request.recv(1024)

        print 'Connected by', self.client_address[0]
        print 'Request is', request

        method = request.split(' ')[0]
        if method == 'GET':
            # self.request.sendall({'message': 'not support'})
            self.request.sendall('get')
        elif method == 'POST':
            form = request.split('\r\n')
            idx = form.index('')  # Find the empty line
            entry = form[idx:]  # Main content of the request

            str_list = entry[-1].split('&')
            key_value_list = map(lambda temp: temp.split('='), str_list)
            self.request.sendall(dict(key_value_list))


class HttpServer(object):
    def start(self, host='127.0.0.1', port=8005):
        # Create the server
        self._server = SocketServer.TCPServer((host, port), MyTCPHandler)
        # Start the server, and work forever
        self._server.serve_forever()

    def stop(self):
        self._server.shutdown()


server = HttpServer()
server.start()