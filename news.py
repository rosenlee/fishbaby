# -*- coding:utf-8 -*-

"""

Created on 2019/01/21

@author: Rosen Lee

@group : 

@contact: rosenlove@qq.com

"""


import json

import time

import sys

import random

import datetime

import MySQLdb as mdb
import traceback
#import basicData

reload(sys)

sys.setdefaultencoding('utf-8')



"""


CREATE TABLE `news` (
 `id` int unsigned not NULL  AUTO_INCREMENT PRIMARY KEY ,
 `content`    text     not null comment '内容',
 `cursor`   varchar(32)      null comment '访问API时的标号',
 `item_id`   int unsigned     not null comment 'API返回的标号',
 `create_date`    datetime  default now()  comment '输入时间',
 `display_time`   datetime    comment '展示时间',
 `score`   int comment '权重',
 `create_by`      varchar(32)    comment '输入者',
UNIQUE INDEX `item_id_index`(`item_id`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8;






1547970922
https://api-prod.wallstreetcn.com/apiv1/content/lives?channel=global-channel&client=pc&limit=20&first_page=false&accept=live%2Cvip-live&cursor=1547970922
"""

try:

    from urllib.request import urlopen, Request

except ImportError:

    from urllib2 import urlopen, Request

    


class DBAccess:
    def __init__(self):
        self.conn = None
        
    def __del__( self ):
        if self.conn is not None:
            self.conn.close()
            
    def commit( self ):
        try:
            self.conn.commit()
            return True
        except mdb.Error, e:
            self.conn.rollback()
            print "Error %d: %s" % (e.args[0], e.args[1])
            return False
        
    def connect( self, host='localhost', user='mvno', passwd='123456', db='RECORD', port=3306 ):
        try:
            self.conn = mdb.connect(host, user, passwd, db, port)
            self.conn.cursor().execute("set names utf8")
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return False
        return  True
        
    def Query( self, sql ):
        try:
            cursor = self.conn.cursor(mdb.cursors.DictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return None
            
    def Insert( self, sql ):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            return True
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return False
    
    def Delete( self, sql ):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            return True
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return False

    
    def QueryNews( self, pointNum):
        sql = "SELECT * from `news` where `cursor`='%s' " %( pointNum);
        print sql
        return self.Query(sql)

    def QueryMinCursor( self):
        sql = "SELECT min(`cursor`) as min from `news`" ;
        print sql
        return self.Query(sql)
       
       
    def QueryAreaInfo( self ):
        try:
            area_map = {}
            sql = 'select PROVINCECODE,CITYCODE,NUMBERH3 from if.INT_VOP_FILENOSECT'
            records = self.QueryBill(sql)
            if records is None:
                print "查询号段表出错！"
                return False
            for row in records:
                area_map[row["NUMBERH3"]] = [row["PROVINCECODE"],row["CITYCODE"]]
            return area_map
        except:
            traceback.print_exc()   
            return None
        
    def InsertNews( self , content, displayTime, cursor, itemId, score):
        try:
            sql = '''insert into `news` (`content`, `cursor`, `display_time`, `item_id`, `score`) values( '%s', '%s', '%s', '%s', '%s');''' % (content , cursor ,displayTime, itemId, score);
	    #print (sql)
            records = self.Insert(sql)
            if records is None:
	        print (sql)
                print "插入数据出错！"
                return False
            if(True == records) :
                self.commit();
            return True
        except:
            traceback.print_exc()   
            return None
            	
	


def getJsonText(url):

    time.sleep(random.randint(1, 23)*3)

    request = Request(url)

    try:

        text = urlopen(request, timeout=100).read()
        #result = unicode(text,'GBK').encode('UTF-8')
        #result = text.encode('gbk', 'ignore')

        return text

    except :

        print ("url Exception")
	traceback.print_exc() 

        return None

        	

org_url="https://api-prod.wallstreetcn.com/apiv1/content/lives?channel=global-channel&client=pc&limit=60&first_page=false&accept=live%2Cvip-live&cursor="
#cursor = '1547970922'


#st =  time.localtime(int(cursor))
#displayTime = time.strftime("%Y-%m-%d %X", st)


#url = org_url + cursor

#page = getJsonText(url)
#print(page)
#jsonText = json.loads(page)

#print (jsonText['data']['items'][1]['content'])
#print (displayTime)

def GetPage(cursor):
    url = org_url + cursor
    page = getJsonText(url)
    jsonText = json.loads(page)
    
    itemNum = len(jsonText['data']['items'])
    #print(itemNum)
    for i in range(itemNum) :
     
        content = jsonText['data']['items'][i]['content_text']
   
        display_time  = jsonText['data']['items'][i]['display_time']
        st =  time.localtime(int(display_time))
        displayTime = time.strftime("%Y-%m-%d %X", st)
        itemId=jsonText['data']['items'][i]['id']
        score=jsonText['data']['items'][i]['score']
 
        ret = db_record.InsertNews(content.encode('utf-8'), displayTime, cursor, itemId, score)
        print(ret)
    print("next_cursor", jsonText['data']['next_cursor'])
 
    return jsonText['data']['next_cursor'].encode('utf-8');
        

#param: period, 区间。
# startSecond， 从哪一秒开始？
def LoopGetRecords(period, startSecond):
    try:
	maxSeconds = 60*60*24*int(period); #获取 n 天的数据
	cursorNum = int(startSecond);
	while(cursorNum is not None) :
#print(int(time.time()))
#print(cursorNum)
	   if(int(startSecond) - int(cursorNum) > maxSeconds):
		print("超期，不再继续") ;
		break;
	   cursorNum = GetPage(str(cursorNum));

    except : 
        print ("url Exception")
        traceback.print_exc()

        return None

#

if __name__ == '__main__':
#
    if len(sys.argv) > 2 and sys.argv[1] == "-r":
       print ("read mode")
       
       # table = OpenFile(sys.argv[2]);
       
       exit()
    
    db_record = DBAccess()
    db_record.connect('127.0.0.1', 'name', 'password', 'data', 3306)
   
    
    #maxSeconds = 60*60*24*7 #获取7天的数据

    cursorNum = int(time.time())
    #while(cursorNum is not None) :
       #print(int(time.time()))
       #print(cursorNum)
    #   if(int(time.time()) - int(cursorNum) > maxSeconds):
#           print("超期，不再继续") 
#           break;
    #   cursorNum = GetPage(str(cursorNum))
#       currentTime = int(time.time())
#       cursorNum = GetPage(str(currentTime))
#       records = db_record.QueryNews(cursorNum);    
       #print(records)

    LoopGetRecords(1, cursorNum);
    ret = db_record.QueryMinCursor();
    print(ret)
    if ret is not None:
        #print(ret[0]['min'])
        minCursorNum = ret[0]['min']
        LoopGetRecords(7, minCursorNum);
     
    print ("end", time.asctime( time.localtime(time.time()) ))

       

   

