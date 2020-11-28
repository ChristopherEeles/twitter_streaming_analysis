from pykafka import KafkaClient

# Connect to the kafka server running on Amazon EC2 virtual machine
client = KafkaClient(hosts='18.221.88.167:9092', zookeeper_hosts='18.221.88.167:2181')