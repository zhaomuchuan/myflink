# -*- coding: UTF-8 -*-

from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin import NewTopic
import json


# 写入消息
def send_msg(topic='mykafka', msg=None):
    producer = KafkaProducer(bootstrap_servers='6.86.2.170:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    if msg is not None:
        producer.send(topic, msg)
        # future.get()


# 读取消息
def get_msg(topic='mykafka'):
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest',bootstrap_servers='6.86.2.170:9092')
    for message in consumer:
        print(message)


# 查询所有Topic
def list_topics():
    global_consumer = KafkaConsumer(bootstrap_servers='6.86.2.170:9092')
    topics = global_consumer.topics()
    return topics


# 创建Topic
def create_topic(topic='mykafka'):
    admin = KafkaAdminClient(bootstrap_servers='6.86.2.170:9092')
    topics = list_topics()
    if topic not in topics:
        topic_obj = NewTopic(topic, 1, 1)
        admin.create_topics(new_topics=[topic_obj])


# 删除Topic
def delete_topics(topic='mykafka'):
    admin = KafkaAdminClient(bootstrap_servers='6.86.2.170:9092')
    topics = list_topics()
    if topic in topics:
        admin.delete_topics(topics=[topic])


if __name__ == '__main__':
    topic = 'user'
    topics = list_topics()
    print(topics)

    msgs = [
            {'a': 'b', 'b': 2, 'c': 1, 'time': '2020-03-28T21:48:14Z'}
            ]
    for msg in msgs:
        send_msg(topic, msg)