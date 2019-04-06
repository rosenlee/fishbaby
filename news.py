# -*- coding:utf-8 -*-

"""

Created on 2019/01/21

@author: Rosen Lee

@group : 



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
 `tag`   varchar(1024) comment 'tag',
 `source`   varchar(8) default 'wsj' comment 'wsj or sina',
 `create_by`      varchar(32)    comment '输入者',
  UNIQUE INDEX `item_id_index`(`item_id`, `source`) USING BTREE,
  INDEX `tag_index`(`tag`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8;



,


1547970922
https://api-prod.wallstreetcn.com/apiv1/content/lives?channel=global-channel&client=pc&limit=20&first_page=false&accept=live%2Cvip-live&cursor=1547970922

http://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&_=1548341385078
"""

try:

    from urllib.request import urlopen, Request

except ImportError:

    from urllib2 import urlopen, Request

    
'''
get num of day 
'''

def getNumOfDay():
    a = time.time()
    dayNum = int (time.strftime('%j', time.localtime(a)) )
    return dayNum


'''
get num of year, from 2019
'''
def getNumOfYear():
    a = time.time()
    yearNum = int (time.strftime('%y', time.localtime(a)) )
    return yearNum - 19;


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
        sql = "SELECT min(`cursor`) as min from `news` where source = 'wsj' ;" ;
        print sql
        return self.Query(sql)

    def QueryMinId( self, source):
        sql = "SELECT if(min(`item_id`) is null, 0, min(`item_id`)) as min from `news` where `source` = '%s' ;" % (source);
        print sql
        return self.Query(sql)

    def QueryMaxId( self, source):
        sql = "SELECT if(max(`item_id`) is null, 0, max(`item_id`)) as max from `news` where `source` = '%s' ;" % (source);
        print sql
        return self.Query(sql)
       
       
        
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
            	
    def InsertNewsSina( self , content, displayTime, cursor, itemId, score,tag, source):
        try:
            sql = '''insert into `news` (`content`, `cursor`, `display_time`, `item_id`, `score`, `tag`, `source`) values( '%s', '%s', '%s', '%s', '%s', '%s', '%s');''' % (content , cursor ,displayTime, itemId, score,tag, source);
	    print (sql)
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

    time.sleep(random.randint(1, 13)*1)

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

#sina_url="http://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&_=1548341385078"

#st =  time.localtime(int(cursor))
#displayTime = time.strftime("%Y-%m-%d %X", st)

#sinaMinId = 0

#wsj
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
        

#sina
'''
#返回 页面信息

"page_info": {
	"totalPage": 20,
	"pageSize": 20,
	"prePage": 1,
	"nextPage": 2,
	"firstPage": 1,
	"lastPage": 20,
	"totalNum": 400,
	"pName": "page",
	"page": 1,
	"max_id": 1112501,
	"min_id": 1112481,
	"last_id": 1112481,
},

'''
def GetPageSina(cursor):
    sina_url="http://zhibo.sina.com.cn/api/zhibo/feed?zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&page_size=20&page="
    url = sina_url + str(cursor)
    print('url = ', url)
    page = getJsonText(url)
    jsonText = json.loads(page)
   
    resultCode = jsonText['result']['status']['code']
    currentTime = jsonText['result']['timestamp']
    #print( "resultCode = ", resultCode )
    #print( "current-time = ", currentTime)

    dataNode = jsonText['result']['data']['feed']
    itemNum = len(dataNode['list'])
    pageInfo = dataNode['page_info']
    itemId=''
    #print('page-size = ', pageInfo['pageSize'])
    #print('itemNum', itemNum)
    for i in range(itemNum) : 
        content = dataNode['list'][i]['rich_text'].encode('utf-8')
        #content = dataNode['list'][i]['rich_text'].encode('utf-8').decode('unicode_escape')
        displayTime  = dataNode['list'][i]['create_time'].encode('utf-8')
        itemId= dataNode['list'][i]['id']
        score= 1

        #get tags 
        tagNum = len(dataNode['list'][i]['tag'])
        tagStr = ""
        for t in range(tagNum):
           #f
           if(9 == int(dataNode['list'][i]['tag'][t]['id'])):
               score = 2
           tagStr = tagStr + "," + dataNode['list'][i]['tag'][t]['name'].encode('utf-8')
 
        ret = db_record.InsertNewsSina(content.encode('utf-8'), displayTime, cursor, itemId, score, tagStr, 'sina')
        print( content )
        print( tagStr )
#        print(displayTime, cursor, itemId, score,tagStr)
 
    pageInfo['max_id'] = dataNode['max_id']
    pageInfo['min_id'] = dataNode['min_id']
    #pageInfo['last_id'] = itemId

    return pageInfo



    
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



'''
本次页所在区间最大值， 最小值
数据表中的最大值，最小值
数据越新，id的数值越大。
由于数据在更新，页码包括的数据也可能变化。
本页的最大值 减去  数据表的最大值，即是本轮要更新的数据差。

历史记录
数据表中的最小值 减去 页面返回 的最小值，即为可更新的数据差 。

pageNum = (pageMax - tableMax )/pageSize 

142930-120849

每次脚本执行最多更新 页数为30页，每页为20条。

'''
def LoopGetRecordsSina(period, startPage):
    try:

        pageSize = 20
        sinaMinId = 0
        idRet = db_record.QueryMinId('sina')
        if idRet is not None:
           sinaMinId = idRet[0]['min']

        sinaMaxId = 0
        idRet = db_record.QueryMaxId('sina')
        if idRet is not None:
           sinaMaxId = idRet[0]['max']
    

	maxPageNum = (int(sinaMaxId) - int(sinaMinId))/pageSize + int(period); #获取 n 页的数据
	pageNum = int(startPage);
        #用来记录上次的页码，为了防止出现死循环
        lastPage = pageNum
	while(pageNum is not None) :

           print('get page-index = ', pageNum)
           #print('start-page = ', startPage)
           print('max-page = ', maxPageNum)
	   if(int(pageNum) - int(startPage) > maxPageNum):
		print("----超期，不再继续") ;
		break;
	   pageInfo = GetPageSina(str(pageNum));
           pageNum = pageInfo['nextPage']
           lastId = pageInfo['min_id']
           #if(1==startPage):
               #maxPageNum = (pageInfo['max_id'] - sinaMaxId)/pageSize + 1 
           #    print('max-page-num = ', maxPageNum)
           #    startPage = 0 

           print('last-id = ', lastId)
           print('table-max-id = ', sinaMaxId)
           if( lastId < sinaMaxId):
                #向下是重复了
                print('----到了最近的历史数据区间上限，不再继续')
                #顺便 计算历史记录从哪里开始继续找
                print(lastId, sinaMinId)
                #nextPage = ( lastId - sinaMinId )/pageSize - 1
                idRet = db_record.QueryMinId('sina')
                if idRet is not None:
                    sinaMinId = idRet[0]['min']

               #跳过重复页面
                pageNum = pageNum + (lastId - sinaMinId )/pageSize

           print('last-page = ', lastPage)
           #判断是否出现重复页码
           if(pageNum <= lastPage):
               pageNum = pageNum + (lastPage-pageNum) + 1
           lastPage = pageNum

           print('next-page = ', pageNum)

    except : 
        print ("url Exception")
        traceback.print_exc()

        return None

'''
 another version of get record from sina
'''
def LoopGetRecordsSina_V2(period, startPage):
    try:

        historyStartPage=6550
        pageSize = 20
        sinaMinId = 0
        idRet = db_record.QueryMinId('sina')
        if idRet is not None:
           sinaMinId = idRet[0]['min']

        sinaMaxId = 0
        idRet = db_record.QueryMaxId('sina')
        if idRet is not None:
           sinaMaxId = idRet[0]['max']
       
        todayNum = getNumOfDay()
        yearNum = getNumOfYear()
        #96 是指从4月6日开始。
        maxPageNum = (todayNum - 96 +1) *period +  historyStartPage + yearNum*400;
        historyPageNum = (todayNum - 96) *period +  historyStartPage + yearNum*400;
    

#	maxPageNum = (int(sinaMaxId) - int(sinaMinId))/pageSize + int(period); #获取 n 页的数据
	pageNum = int(startPage);
        #用来记录上次的页码，为了防止出现死循环
        lastPage = pageNum
	while(pageNum is not None) :

           print('get page-index = ', pageNum)
           #print('start-page = ', startPage)
           print('max-page = ', maxPageNum)
	   if(int(pageNum) - int(startPage) > maxPageNum):
		print("----超期，不再继续") ;
		break;
	   pageInfo = GetPageSina(str(pageNum));
           pageNum = pageInfo['nextPage']
           lastId = pageInfo['min_id']
           #if(1==startPage):
               #maxPageNum = (pageInfo['max_id'] - sinaMaxId)/pageSize + 1 
           #    print('max-page-num = ', maxPageNum)
           #    startPage = 0 

           print('last-id = ', lastId)
           print('table-max-id = ', sinaMaxId)
           if( lastId < sinaMaxId):
                #向下是重复了
                print('----到了最近的历史数据区间上限，不再继续')
                #顺便 计算历史记录从哪里开始继续找
                print(lastId, sinaMinId)
                #nextPage = ( lastId - sinaMinId )/pageSize - 1
               #跳过重复页面
                pageNum = historyPageNum

           print('last-page = ', lastPage)
           #判断是否出现重复页码
           if(pageNum <= lastPage):
               pageNum = pageNum + (lastPage-pageNum) + 1
           lastPage = pageNum

           print('next-page = ', pageNum)

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
    db_record.connect('127.0.0.1', 'db', 'passwd', 'db', 3306) 

    #nextPage = LoopGetRecordsSina(35, 1);
    LoopGetRecordsSina_V2(35,1)
#    sys.exit()



#    exit();
    # 开始处理华尔街 消息
    cursorNum = int(time.time())

    LoopGetRecords(1, cursorNum);
    ret = db_record.QueryMinCursor();
    print(ret)
    if ret is not None:
        #print(ret[0]['min'])
        minCursorNum = ret[0]['min']
        LoopGetRecords(7, minCursorNum);
     
    print ("end", time.asctime( time.localtime(time.time()) ))

       

   

