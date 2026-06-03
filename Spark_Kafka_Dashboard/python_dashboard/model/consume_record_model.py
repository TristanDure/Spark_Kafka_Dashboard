import time

# 消费记录模型
class consume_record():
    name=str
    gender=str
    phone=str
    city=str
    goods_type=str
    amount=int
    payment=str
    consume_time=time

    def __init__(self, name='', gender='', phone='', city='', goods_type='', amount=0, payment='', consume_time=''):
        self.name = name
        self.gender = gender
        self.phone = phone
        self.city = city
        self.goods_type = goods_type
        self.amount = amount
        self.payment = payment
        self.consume_time = consume_time


# 用于序列化的方法
def record2dict(record):
    return {
        'name': record.name,
        'gender': record.gender,
        'phone': record.phone,
        'city': record.city,
        'goods_type': record.goods_type,
        'amount': record.amount,
        'payment': record.payment,
        'consume_time': record.consume_time
    }