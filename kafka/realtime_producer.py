import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

crops = ["RICE", "WHEAT", "OILSEEDS"]

print("🚜 Real-time farming data producer started...")

while True:
    data = {
        "year": random.randint(2018, 2024),
        "crop": random.choice(crops),
        "area": round(random.uniform(50, 600), 2),
        "production": round(random.uniform(30, 500), 2),
        "annual_rainfall": round(random.uniform(600, 2000), 2),
        "kharif_rainfall": round(random.uniform(300, 1200), 2),
        "soil_moisture_index": round(random.uniform(0.2, 1.0), 3),
        "soil_quality_index": round(random.uniform(0.3, 1.2), 3)
    }

    producer.send("agriculture_stream", data)
    print("Sent:", data)
    time.sleep(5)

