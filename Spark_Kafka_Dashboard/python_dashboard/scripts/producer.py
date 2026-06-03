# coding: utf-8
from kafka import KafkaProducer
import time

from setting import BaseConfig
from util.DataUtil import generateData

bootstrap_servers = BaseConfig.KAFKA_ADDRESS
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# kafka生产者
def Producer():
    topic = BaseConfig.PRODUCER_TOPIC
    interval = BaseConfig.GENERATE_INTERVAL
    while True:
        # 数据是本地实时生成的随机数据
        data = generateData()
        # 发送到名为test的topic上
        producer.send(topic, data.encode('utf-8'))
        # 暂停2S继续发
        time.sleep(interval)
        # print("kafka数据传输成功")

