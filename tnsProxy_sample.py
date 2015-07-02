# -*- coding: utf-8 -*-
'''
Python oracle Tns proxy.
https://github.com/bombrose/TnsProxy
'''
import socket, thread, select

HEADLEN = 8
HOST, PORT, TIMEOUT = "", 7522, 3600  #代理服务器地址、端口、超时（秒）
TNS_HOST, TNS_PORT = "127.0.0.1", 1521  #oracle服务器地址、端口
class ConnectionHandler:
    def __init__(self, connection, address, timeout, tns_host, tns_port):
        self.tns_host, self.tns_port = tns_host, tns_port
        self.client = connection
        self.timeout = timeout
        self.method_def = dict(zip([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], \
                                   ['CONNECT', 'ACCEPT', 'ACK', 'REFUTE', 'REDIRECT', 'DATA', 'NULL', 'OTHER1', 'ABORT',
                                    'OTHER2', 'RESEND', 'MARKER', 'ATTENTION', 'CONTROL']))
        self.client_buffer = self.__get_buffer__(self.client)
        self.transfer()
        self.client.close()
        self.target.close()

    def __get_method__(self, buf):
        method = self.method_def.get(ord(buf[4])) \
            if len(buf) >= 5 and self.method_def.has_key(ord(buf[4])) else "NONE"
        return method

    def __get_buffer__(self, soc):
        header = soc.recv(HEADLEN)
        bufferLen = ord(header[0]) * 256 + ord(header[1]) - HEADLEN if len(header) >= 2 else 0
        data = header + soc.recv(bufferLen) if bufferLen > 0 else header
        return data

    def __send_buffer__(self, soc, buf):
        soc.send(buf)

    def __connect_target__(self):
        self.client_buffer = self.client_buffer.replace(str(PORT), str(self.tns_port))  #更改端口
        (soc_family, _, _, _, address) = socket.getaddrinfo(self.tns_host, self.tns_port)[0]
        self.target = socket.socket(soc_family)
        self.target.connect(address)

    def transfer(self):
        method = self.__get_method__(self.client_buffer)
        if method == 'CONNECT':
            self.__connect_target__()
        self.__send_buffer__(self.target, self.client_buffer)
        self.client_buffer = ''
        self.__read_write__()

    def __read_write__(self):
        time_out_max = self.timeout / 2
        socs = [self.client, self.target]
        count = 0
        while 1:
            try:
                count += 1
                (recv, _, error) = select.select(socs, [], socs, 2)
                if error:
                    break
                if recv:
                    for in_ in recv:
                        data = self.__get_buffer__(in_)
                        out = self.target if in_ is self.client else self.client
                        if data:
                            self.__send_buffer__(out, data)
                            count, data = 0, ""
                if count == time_out_max:
                    break
            except Exception, e:
                print traceback.format_exc()
                break

def start_server(host=HOST, port=PORT, timeout=TIMEOUT, handler=ConnectionHandler, tns_host=TNS_HOST,tns_port=TNS_PORT):
    soc = socket.socket(socket.AF_INET)
    soc.bind((host, port))
    print u"server started %s:%d." % (host, port)  #debug
    soc.listen(0)
    while 1:
        thread.start_new_thread(handler, soc.accept() + (timeout, tns_host, tns_port,))

if __name__ == '__main__':
    start_server()
