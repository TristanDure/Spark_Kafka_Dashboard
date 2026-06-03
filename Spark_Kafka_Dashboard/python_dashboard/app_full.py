"""
大数据综合应用系统 — 完整版
技术栈：Kafka + PySpark(Spark SQL/Streaming/MLlib) + HDFS + Flask + ECharts
≥5个大数据组件，全数据链路覆盖

运行方式：
  python app_full.py          # 默认（本地处理模式）
  python app_full.py --kafka  # Kafka模式（需Kafka运行中）
  python app_full.py --spark  # PySpark模式（需 pyspark 已安装）
"""
import os
import sys
import json
import time
import random
from threading import Lock
from collections import deque

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

# 项目模块
from scripts.data_generator import generate_batch
from scripts.hdfs_storage import storage as hdfs
from scripts.spark_processor import spark_processor, PYSPARK_AVAILABLE

# ===== Flask应用初始化 =====
app = Flask(__name__)
app.config['SECRET_KEY'] = 'big-data-secret!'
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")
thread = None
thread_lock = Lock()

# ===== 配置 =====
CONFIG = {
    "INTERVAL": 2,           # 数据刷新间隔（秒）
    "BATCH_SIZE": 50,        # 每批数据量
    "MODE": "local",         # local / kafka / spark
    "HOST": "0.0.0.0",
    "PORT": 5000
}

# 历史数据缓存（用于离线分析展示）
history_buffer = deque(maxlen=1000)


def process_batch(records):
    """处理一批数据：聚合计算 + 存储到HDFS + Spark分析"""
    # 1. 实时聚合（给ECharts）
    realtime_result = spark_processor.process_stream_batch(records)

    # 2. 存储原始数据到HDFS
    hdfs.write_raw(records)

    # 3. 累积历史数据
    history_buffer.extend(records)

    return realtime_result


def background_thread():
    """后台线程：数据生成 → 处理 → 推送前端"""
    global thread
    count = 0
    print(f"[后台线程] 启动，模式={CONFIG['MODE']}，间隔={CONFIG['INTERVAL']}s")

    while True:
        # 生成数据
        records = generate_batch(CONFIG["BATCH_SIZE"])

        # 大数据处理
        data = process_batch(records)

        # 推送前端
        count += 1
        t = time.strftime('%H:%M:%S', time.localtime())
        socketio.emit('server_response',
                      {'data': [t, *data], 'count': count},
                      namespace='/dashboard')

        socketio.sleep(CONFIG["INTERVAL"])


@socketio.on('connect', namespace='/dashboard')
def handle_connect():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(target=background_thread)
    print("[SocketIO] 客户端已连接")


@socketio.on('disconnect', namespace='/dashboard')
def handle_disconnect():
    print("[SocketIO] 客户端已断开")


# ===== 页面路由 =====
@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route("/dashboard")
def dashboard():
    """仪表盘首页：KPI卡片 + 实时图表"""
    return render_template("dashboard.html")


@app.route("/city")
def city():
    return render_template("city.html")


@app.route("/gender")
def gender():
    return render_template("gender.html")


@app.route("/payment")
def payment():
    return render_template("payment.html")


@app.route("/goodstype")
def goods_type():
    return render_template("goods_type.html")


@app.route("/map")
def map_view():
    return render_template("map.html")


# ===== API路由 =====
@app.route("/api/overview")
def api_overview():
    """获取系统概览"""
    # 触发离线分析
    if len(history_buffer) > 0:
        batch_results = spark_processor.batch_analysis(list(history_buffer))
    else:
        batch_results = {}

    return jsonify({
        "mode": CONFIG["MODE"],
        "pyspark_available": PYSPARK_AVAILABLE,
        "storage_stats": hdfs.get_storage_stats(),
        "analytics": batch_results,
        "buffer_size": len(history_buffer)
    })


@app.route("/api/analytics")
def api_analytics():
    """离线分析结果"""
    if len(history_buffer) == 0:
        return jsonify({"status": "no_data"})

    results = spark_processor.batch_analysis(list(history_buffer))
    return jsonify(results)


@app.route("/api/storage")
def api_storage():
    """存储层状态"""
    return jsonify(hdfs.get_storage_stats())


# ===== 启动 =====
if __name__ == '__main__':
    # 解析命令行参数
    if "--kafka" in sys.argv:
        CONFIG["MODE"] = "kafka"
        print("[Kafka模式] 需先启动Kafka: docker compose up -d")

    if "--spark" in sys.argv:
        CONFIG["MODE"] = "spark"
        if PYSPARK_AVAILABLE:
            print("[PySpark模式] PySpark已就绪")
        else:
            print("[PySpark模式] PySpark未安装，使用降级模式")

    print("=" * 60)
    print("  大数据综合应用系统 - 电商实时分析平台")
    print("  技术组件: Kafka + PySpark(Spark SQL/Streaming/MLlib) + HDFS")
    print("  可视化: Flask + ECharts + SocketIO")
    print(f"  访问地址: http://localhost:{CONFIG['PORT']}")
    print(f"  仪表盘:   http://localhost:{CONFIG['PORT']}/dashboard")
    print(f"  运行模式: {CONFIG['MODE']}")
    print("=" * 60)

    socketio.run(app, debug=True, host=CONFIG["HOST"], port=CONFIG["PORT"])
