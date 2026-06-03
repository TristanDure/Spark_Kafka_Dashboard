"""
数据生成器：模拟电商用户行为数据
生成的数据可送入 Kafka / PySpark / 本地处理
"""
import json
import time
import random
import uuid

# ===== 数据字典 =====
GENDERS = ["男", "女"]
CITIES = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "南京"]
GOODS_TYPES = ["交通", "餐饮", "娱乐", "教育", "住房", "其他"]
PAYMENTS = ["银联", "支付宝", "微信", "现金"]
AGE_GROUPS = ["18-25", "26-35", "36-45", "46-55", "55+"]
DEVICES = ["iOS", "Android", "PC", "H5"]
ACTIONS = ["view", "click", "add_cart", "purchase", "pay"]

SURNAME = ["赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈",
           "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许",
           "何", "吕", "张", "孔", "曹", "严", "华", "金", "魏", "陶"]


def generate_name(gender="男"):
    """生成随机中文名"""
    name = random.choice(SURNAME)
    if gender == "男":
        chars = "伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信子杰涛昌成康星光天达安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘"
    else:
        chars = "秀娟英华慧巧美娜静淑惠珠翠雅芝玉萍红娥玲芬芳燕彩春菊兰凤洁梅琳素云莲真环雪荣爱妹霞香月莺媛艳瑞凡佳嘉琼勤珍贞莉桂娣叶璧璐娅琦晶妍茜秋珊莎锦黛青倩婷姣婉娴瑾颖露瑶怡婵雁蓓纨仪荷丹蓉眉君琴蕊薇菁梦岚苑婕馨瑗琰韵融园艺咏卿聪澜纯毓悦昭冰爽琬茗羽希宁欣飘育滢馥筠柔竹霭凝晓欢霄枫芸菲寒伊亚宜可姬舒影荔枝思丽"
    idx = random.randint(0, len(chars) - 1)
    if idx % 2 == 0:
        name += chars[idx:idx+2] if idx + 2 <= len(chars) else chars[idx]
    else:
        name += chars[idx]
    return name


def generate_user():
    """生成一个用户信息"""
    gender = random.choice(GENDERS)
    return {
        "user_id": str(uuid.uuid4())[:8],
        "name": generate_name(gender),
        "gender": gender,
        "age_group": random.choice(AGE_GROUPS),
        "city": random.choice(CITIES),
        "device": random.choice(DEVICES)
    }


def generate_transaction(user=None):
    """生成一条交易记录"""
    if user is None:
        user = generate_user()
    return {
        "user_id": user["user_id"],
        "name": user["name"],
        "gender": user["gender"],
        "age_group": user["age_group"],
        "city": user["city"],
        "device": user["device"],
        "goods_type": random.choice(GOODS_TYPES),
        "amount": round(random.uniform(1, 9999), 2),
        "payment": random.choice(PAYMENTS),
        "action": random.choice(ACTIONS),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    }


def generate_batch(count=50):
    """生成一批交易记录"""
    return [generate_transaction() for _ in range(count)]


def generate_json_batch(count=50):
    """生成一批数据的JSON字符串（用于Kafka传输）"""
    records = generate_batch(count)
    return json.dumps(records, ensure_ascii=False)


if __name__ == '__main__':
    # 测试
    batch = generate_batch(5)
    print(json.dumps(batch, ensure_ascii=False, indent=2))
