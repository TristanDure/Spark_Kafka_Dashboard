import json
import time
import random

from model.consume_record_model import consume_record, record2dict
from setting import BaseConfig

genders = "男,女".split(",")
cities = "北京,上海,广州,深圳".split(",")
goods_types = "交通,餐饮,娱乐,教育,住房,其他".split(",")
payments = "银联,支付宝,微信,现金".split(",")
record_count = BaseConfig.RECORD_COUNT

# 生成随机数据，最后转为json格式
def generateData():
    records = []
    for i in range(record_count):
        gender = genders[random.randint(0,1)]
        name = generateName(gender)
        phone = generatePhone()
        city = cities[random.randint(0,len(cities)-1)]
        goods_type = goods_types[random.randint(0, len(goods_types) - 1)]
        amount = random.randint(0,9999)
        payment = payments[random.randint(0, len(payments) - 1)]
        consume_time = time.asctime(time.localtime(time.time()))
        records.append(consume_record(name, gender, phone, city, goods_type, amount, payment, consume_time))

    return json.dumps(records,default=record2dict,ensure_ascii=False)


def generateName(gender):
    Surname = ["赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许",
               "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "戚", "谢", "邹", "喻", "柏", "水", "窦", "章", "云",
               "苏", "潘", "葛", "奚", "范", "彭", "郎",
               "鲁", "韦", "昌", "马", "苗", "凤", "花", "方", "俞", "任", "袁", "柳", "酆", "鲍", "史", "唐", "费", "廉", "岑", "薛", "雷",
               "贺", "倪", "汤", "滕", "殷",
               "罗", "毕", "郝", "邬", "安", "常", "乐", "于", "时", "傅", "皮", "卞", "齐", "康", "伍", "余", "元", "卜", "顾", "孟", "平",
               "黄", "和",
               "穆", "萧", "尹", "姚", "邵", "湛", "汪", "祁", "毛", "禹", "狄", "米", "贝", "明", "臧", "计", "伏", "成", "戴", "谈", "宋",
               "茅", "庞", "熊", "纪", "舒",
               "屈", "项", "祝", "董", "梁", "杜", "阮", "蓝", "闵", "席", "季"]
    name = Surname[random.randint(0,len(Surname)-1)]
    girl = "秀娟英华慧巧美娜静淑惠珠翠雅芝玉萍红娥玲芬芳燕彩春菊兰凤洁梅琳素云莲真环雪荣爱妹霞香月莺媛艳瑞凡佳嘉琼勤珍贞莉桂娣叶璧璐娅琦晶妍茜秋珊莎锦黛青倩婷姣婉娴瑾颖露瑶怡婵雁蓓纨仪荷丹蓉眉君琴蕊薇菁梦岚苑婕馨瑗琰韵融园艺咏卿聪澜纯毓悦昭冰爽琬茗羽希宁欣飘育滢馥筠柔竹霭凝晓欢霄枫芸菲寒伊亚宜可姬舒影荔枝思丽 "
    boy = "伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信子杰涛昌成康星光天达安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘"
    if gender=="男":
        j = random.randint(0,boy.__len__() - 1)
        if j % 2 == 0:
            name = name + boy[j: j + 2]
        else:
            name = name + boy[j:j + 1]
    else:
        j = random.randint(0,girl.__len__() - 1)
        if j % 2 == 0:
            name = name + girl[j: j + 2]
        else:
            name = name + girl[j:j + 1]
    return name

def generatePhone():
    telFirst = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",")
    index = random.randint(0, len(telFirst) - 1)
    first = telFirst[index]
    second = str(random.randint(1, 9999))
    third = str(random.randint(1, 9999))
    return first + second + third