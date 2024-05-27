# -*- coding: UTF-8 -*-

from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin import NewTopic
import json
from kafka.structs import TopicPartition

passwd = '123456'
def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]
    return inner


# 写入消息
# @singleton
class MyKafka:

    offset = 4

    def send_msg(self, topic='mykafka', msg=None):
        producer = KafkaProducer(bootstrap_servers='6.86.2.170:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        if msg is not None:
            producer.send(topic, msg)
            # future.get()


    # 读取消息
    def get_msg(self, topic='mykafka'):
        consumer = KafkaConsumer(topic, auto_offset_reset='earliest',bootstrap_servers='6.86.2.170:9092')
        self.offset = self.offset + 1
        for message in consumer:
            print(message)
            # print('helo insert')


    # 查询所有Topic
    def list_topics(self):
        global_consumer = KafkaConsumer(bootstrap_servers='6.86.2.170:9092')
        topics = global_consumer.topics()
        return topics


    # 创建Topic
    def create_topic(self, topic='mykafka'):
        admin = KafkaAdminClient(bootstrap_servers='6.86.2.170:9092')
        topics = self.list_topics()
        if topic not in topics:
            topic_obj = NewTopic(topic, 1, 1)
            admin.create_topics(new_topics=[topic_obj])


    # 删除Topic
    def delete_topics(self, topic='mykafka'):
        admin = KafkaAdminClient(bootstrap_servers='6.86.2.170:9092')
        topics = self.list_topics()
        if topic in topics:
            admin.delete_topics(topics=[topic])


if __name__ == '__main__':
    topic = 'user'
    inst = MyKafka()
    topics = inst.list_topics()
    print(topics)
    # if topic in topics:
    #     delete_topics()

    # create_topic(topic)
    msgs = [
            {'a': 'a', 'b': 302, 'c': 1, 'time': '2020-03-28T20:48:14Z'},
            {'a': 'a', 'b': 4100, 'c': 1, 'time': '2020-03-28T20:48:14Z'},
            {'a': 'b', 'b': 301, 'c': 1, 'time': '2020-03-28T21:48:14Z'},
            {'a': 'b', 'b': 4100, 'c': 1, 'time': '2020-03-28T21:48:14Z'}
            ]
    for msg in msgs:
        inst.send_msg(topic, msg)
    # print test data
    # inst.delete_topics('access_log')
    inst.get_msg('access_log')
