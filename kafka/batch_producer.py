import json
import csv
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# with open('data/historical/indian_agriculture.csv', 'r') as file:
with open('/home/sunbeam/Downloads/BigData_project/data/historical/indian_agriculture.csv', 'r') as file:

    reader = csv.DictReader(file)
    for row in reader:
        producer.send('agriculture_batch', row)

producer.flush()
print("Historical data successfully sent to Kafka")
