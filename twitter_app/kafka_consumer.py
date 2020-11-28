import tweepy
import json
import re

# ---- 1. Connect to Kafka Server
from pykafka import KafkaClient

# Connect to the kafka server running on Amazon EC2 virtual machine
client = KafkaClient(hosts='18.221.88.167:9092', zookeeper_hosts='18.221.88.167:2181')

# Configure procuder for my topic
topic = client.topics['azn_vaccine']
consumer = topic.get_simple_consumer()

# Get next topic in consumer
consumer.consume()  # This increments the offset, so the next call will get the next message

# To start over from the beginning of your message que, use
consumer.reset_offsets()