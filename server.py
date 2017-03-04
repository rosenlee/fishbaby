#-*- coding: utf-8 -*-
from socket import *
from time import ctime
import sys
import json
import struct as st



def UnpackData(data):
    header = st.unpack("cccxh", data[:6])
    print "UnpackData header len :",  header[-1]
    s = st.unpack("%ds" %(header[-1]), data[6:])
    ret = ""
    for item in s:
        ret += "%s" % (item)
    return ret
    
HOST=''
PORT=12345
BUFSIZ=1024
ADDR=(HOST, PORT)
sock=socket(AF_INET, SOCK_STREAM)

sock.bind(ADDR)

sock.listen(5)
while True:
    print('waiting for connection')
    tcpClientSock, addr=sock.accept()
    print('connect from ', addr)
    while True:
        try:
            data=tcpClientSock.recv(BUFSIZ)
        except:
            print(e)
            tcpClientSock.close()
            break
        if not data:
            break
        if data == "exit":
            sys.exit()
        bdata = bytearray(data)
        #for i in range(len(bdata)):
        #    print "%d:%x . %c " %(i, bdata[i], bdata[i])
        '''
        js = json.loads(data)   
        print js    
       #'''
        data = UnpackData(data)
        s='Hi,you send me :[%s] %s' %(ctime(), data.decode('utf8'))
        tcpClientSock.send(s.encode('utf8'))
        print([ctime()], ':', data.decode('utf8'))
tcpClientSock.close()
sock.close()





