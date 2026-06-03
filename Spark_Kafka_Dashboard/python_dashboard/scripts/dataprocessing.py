import os

from model.consume_record_model import consume_record
from setting import BaseConfig
from util.JsonUtil import json_deserialize2objlist

# 把所有数据都放进一个list里，1-4城市，5-6性别，7-10支付方式，11-16消费类型
# 如果不适用流计算就这么处理，后面做成sparkstreaming
def calculateData(json_data):
    # 先解析json为list
    records = json_deserialize2objlist(json_data, consume_record)
    result = []
    beijing = shanghai= guangzhou = shenzhen = 0
    male = female = 0
    unionpay = ali = wechat = cash = 0
    transport = food = entertainment = education = housing = other = 0
    for record in records:
        if record.city == "北京":
            beijing += record.amount
        if record.city == "上海":
            shanghai += record.amount
        if record.city == "广州":
            guangzhou += record.amount
        if record.city == "深圳":
            shenzhen += record.amount
        if record.gender == "男":
            male += record.amount
        if record.gender == "女":
            female += record.amount
        if record.payment == "银联":
            unionpay += record.amount
        if record.payment == "支付宝":
            ali += record.amount
        if record.payment == "微信":
            wechat += record.amount
        if record.payment == "现金":
            cash += record.amount
        if record.goods_type == "交通":
            transport += record.amount
        if record.goods_type == "餐饮":
            food += record.amount
        if record.goods_type == "娱乐":
            entertainment += record.amount
        if record.goods_type == "教育":
            education += record.amount
        if record.goods_type == "住房":
            housing += record.amount
        if record.goods_type == "其他":
            other += record.amount

    result.append(beijing)
    result.append(shanghai)
    result.append(guangzhou)
    result.append(shenzhen)
    result.append(male)
    result.append(female)
    result.append(unionpay)
    result.append(ali)
    result.append(wechat)
    result.append(cash)
    result.append(transport)
    result.append(female)
    result.append(entertainment)
    result.append(education)
    result.append(housing)
    result.append(other)
    return result

# 解析流计算的数据
def calculateSparkStreaming(data):
    arr0 = data.replace("[","").replace("]","").split(",")
    arr1 = []
    for element in arr0:
        arr1.append(float(element))
    return arr1

# 启动scala的流计算
def callStreaming():
    setting_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+"/setting.py"
    jar_path = BaseConfig.JAR_PATH
    spark_path = BaseConfig.SPARK_HOME
    cmd = spark_path + "spark-submit --class KafkaProcessing "+jar_path + 'DashboardDataProcessing.jar '+setting_path
    os.popen(cmd)

# 保存到数据库中
def save2DB(data):
    # TODO 未来会保存到数据库中
    print("存储成功")