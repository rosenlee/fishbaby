# -*- coding:utf-8 -*-

from flask import Flask
from flask import request
from flask import jsonify
from flask import make_response
import news

import sys
reload(sys)
sys.setdefaultencoding('utf8')

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    return '<h1>hello world python 3</h1>'



# 测试数据暂时存放
tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.route('/add_task', methods=['POST'])
def add_task():
    db_record = news.DBAccess()
    db_record.connect('127.0.0.1', 'dev', 'dev123456', 'dev', 3306)

    print(db_record)
    cur = db_record.QueryMinCursor();
    print(cur)
    return jsonify(cur[0]);
    if not request.json or 'id' not in request.json or 'info' not in request.json:
        print("not found !!!")
        abort(400)
    task = {
        'id': request.json['id'],
        'info': request.json['info']
    }

    tasks.append(task)
    return jsonify({'result': 'success'})


@app.route('/get_task', methods=['GET', 'POST'])
def get_task():
    print('请求头:%s' % request.headers)       #打印结果为请求头信息
    print('请求方式:%s' % request.method)      #GET
    print('请求url地址:%s' % request.url)      # 请求url地址:http://127.0.0.1:5000/
    print('请求数据:%s' % request.data)        # 请求数据:b'{name:"zs",age:18}'

    db_record = news.DBAccess()
    db_record.connect('127.0.0.1', 'dev', 'dev123456', 'dev', 3306)

    print(db_record)
    cur = db_record.QueryMinCursor();
    print(cur)
    print(request)
    #return jsonify(cur)
    return jsonify({'tasks': tasks})
    if not request.args or 'id' not in request.args:
        # 没有指定id则返回全部
        return jsonify(tasks)
    else:
        task_id = request.args['id']
        task = filter(lambda t: t['id'] == int(task_id), tasks)
        return jsonify(task) if task else jsonify({'result': 'not found'})




if __name__ == '__main__':
    app.run(
       host = '0.0.0.0',
       port = 5000,
       debug = True)

                  
