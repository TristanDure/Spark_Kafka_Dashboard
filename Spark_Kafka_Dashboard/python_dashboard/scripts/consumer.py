from kafka import KafkaConsumer

#kafka消费者
from setting import BaseConfig


def Consumer():
    # kafka的topic用test，部署在本机
    # 暂时不用配置文件的方式
    topic = BaseConfig.CONSUMER_TOPIC
    bootstrap_servers = BaseConfig.KAFKA_ADDRESS
    consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers)
    return consumer


