import time
from threading import Lock

from concurrent.futures import ThreadPoolExecutor

from flask import Flask, render_template
from flask_socketio import SocketIO

import setting
from scripts.consumer import Consumer
from scripts.dataprocessing import calculateData, calculateSparkStreaming, callStreaming
from scripts.producer import Producer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=None)
thread = None
executor = ThreadPoolExecutor(5)
thread_lock = Lock()
app.config.from_object(setting.BaseConfig)

# 后台线程 产生数据，然后推送至前端
def background_thread():
    executor.submit(producer_task)
    # TODO 等处理好jar包就启用
    # executor.submit(streaming_task)
    count = 0
    # 是调用流计算还是本地计算
    flag = app.config["PRODUCER_TOPIC"] == app.config["CONSUMER_TOPIC"]
    for msg in Consumer():
        json_data = msg.value.decode('utf-8')
        data = calculateData(json_data) if flag else calculateSparkStreaming(json_data)
        count += 1
        t = time.strftime('%H:%M:%S', time.localtime())  # 获取系统时间（只取分:秒）
        socketio.emit('server_response',
                      {'data': [t, *data], 'count': count},
                      namespace='/dashboard')  # 注意：这里不需要客户端连接的上下文，默认 broadcast = True ！！！！！！！
        # print('socket-io传输数据成功')
        # 休息一下,然后刷新
        socketio.sleep(app.config["GENERATE_INTERVAL"])


# 另一个后台线程，不停生成数据
def producer_task():
    Producer()

def streaming_task():
    callStreaming()


# 与前端建立 socket 连接后，启动后台线程
@socketio.on('connect', namespace='/dashboard')
def test_connect():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(target=background_thread)


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html", async_mode=socketio.async_mode)


@app.route("/city")
def city():
    return render_template("city.html", async_mode=socketio.async_mode)


@app.route("/gender")
def gender():
    return render_template("gender.html", async_mode=socketio.async_mode)


@app.route("/payment")
def payment():
    return render_template("payment.html", async_mode=socketio.async_mode)


@app.route("/goodstype")
def goods_type():
    return render_template("goods_type.html", async_mode=socketio.async_mode)


# 启动辣！
if __name__ == '__main__':
    socketio.run(app, debug=True)
