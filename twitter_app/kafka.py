from pykafka import KafkaClient

# Connect to the kafka server running on Amazon EC2 virtual machine
client = KafkaClient(hosts='3.139.77.133:9092')


