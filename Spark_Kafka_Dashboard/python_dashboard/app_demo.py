"""
Demo版：不依赖Kafka，直接本地生成数据 + ECharts实时大屏
先跑通这个看效果，之后再接入Kafka/Spark/HDFS
"""
import time
import random
import json
from threading import Lock

from flask import Flask, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='threading')
thread = None
thread_lock = Lock()

# ========== 数据生成（模拟交易数据） ==========
GENDERS = ["男", "女"]
CITIES = ["北京", "上海", "广州", "深圳"]
GOODS_TYPES = ["交通", "餐饮", "娱乐", "教育", "住房", "其他"]
PAYMENTS = ["银联", "支付宝", "微信", "现金"]
RECORD_COUNT = 50
INTERVAL = 2  # 每2秒刷新


def generate_records():
    """生成一批随机消费记录"""
    surname = ["赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "褚", "卫", "蒋",
               "沈", "韩", "杨", "朱", "秦", "尤", "许", "何", "吕", "张", "孔", "曹", "严",
               "华", "金", "魏", "陶", "姜", "苏", "潘", "葛", "范", "彭", "郎"]
    records = []
    for _ in range(RECORD_COUNT):
        gender = random.choice(GENDERS)
        city = random.choice(CITIES)
        goods_type = random.choice(GOODS_TYPES)
        amount = random.randint(1, 9999)
        payment = random.choice(PAYMENTS)
        records.append({
            "gender": gender, "city": city,
            "goods_type": goods_type, "amount": amount,
            "payment": payment,
            "time": time.strftime('%H:%M:%S', time.localtime())
        })
    return records


def calculate(records):
    """本地聚合计算：按城市/性别/支付方式/消费类型汇总"""
    result = []
    # 1-4: 城市
    for city in CITIES:
        result.append(sum(r["amount"] for r in records if r["city"] == city))
    # 5-6: 性别
    for g in GENDERS:
        result.append(sum(r["amount"] for r in records if r["gender"] == g))
    # 7-10: 支付方式
    for p in PAYMENTS:
        result.append(sum(r["amount"] for r in records if r["payment"] == p))
    # 11-16: 消费类型
    for gt in GOODS_TYPES:
        result.append(sum(r["amount"] for r in records if r["goods_type"] == gt))
    return result


def background_thread():
    """后台线程：不停生成数据、计算、推送前端"""
    count = 0
    while True:
        records = generate_records()
        data = calculate(records)
        count += 1
        t = time.strftime('%H:%M:%S', time.localtime())
        socketio.emit('server_response',
                      {'data': [t, *data], 'count': count},
                      namespace='/dashboard')
        socketio.sleep(INTERVAL)


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


if __name__ == '__main__':
    print("=" * 50)
    print(" 实时交易数据可视化大屏 (Demo模式)")
    print(f" 访问地址: http://localhost:5000")
    print(" 数据刷新间隔: {}秒".format(INTERVAL))
    print("=" * 50)
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
