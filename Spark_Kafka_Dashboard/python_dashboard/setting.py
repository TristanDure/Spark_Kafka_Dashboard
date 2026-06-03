class BaseConfig(object):
    # 基础配置
    # kafka运行的zookeeper地址
    KAFKA_ADDRESS = 'localhost:9092'
    # 生成数据间隔
    GENERATE_INTERVAL = 2
    # kafka生产者的topic
    PRODUCER_TOPIC = 'test'
    # kafka消费者的topic
    CONSUMER_TOPIC = 'test_consumer'
    # 运行spark的主机地址
    SPARK_HOST = 'local[*]'
    # spark的程序名称
    SPARK_APP = 'DataProcessing'
    # spark
    SPARK_HOME = "/usr/local/spark/bin/"
    JAR_PATH = "/Users/liuyudeng/Desktop/BigDataUnionpay/" \
               "unionpay_dashboard/DashboardDataProcessing/" \
               "out/artifacts/DashboardDataProcessing_jar"
    # kafka的groupid
    GROUP_ID = 'ycliu'
    # 每次生成的消费记录数量
    RECORD_COUNT = 50

class TestConfig(BaseConfig):
    # 测试环境配置
    AAA = 111

class DevConfig(BaseConfig):
    # 开发环境配置
    AAA = 222

class ProConfig(BaseConfig):
    # 线上环境配置
    AAA = 333