#-*- coding: utf-8 -*-
from socket import *
import sys
import struct as st

def GetBytes(data,  dataLen):
    a = st.pack("cccxh%ds" % (dataLen), '1', '1', '0', dataLen, data[:dataLen])
    return a 

dataLen = 10    

class TcpClient:
    HOST='127.0.0.1'
    PORT=12345
    BUFSIZ=1024
    ADDR=(HOST, PORT)
    def __init__(self):
        self.client=socket(AF_INET, SOCK_STREAM)
        self.client.connect(self.ADDR)
        f = open("json.txt", "r")
        lines = f.readlines()
        
        while True:
            data = None
            if len(sys.argv) >1:
                dataLen=int(sys.argv[1])
            #else:
            data = "".join(lines)               
            data = GetBytes(data,  dataLen)
            print  type(data)
            self.client.send(data.encode('utf8'))
            data=self.client.recv(self.BUFSIZ)
            if not data:
                break
            print(data.decode('utf8'))
            break;
            
if __name__ == '__main__':
    client=TcpClient()