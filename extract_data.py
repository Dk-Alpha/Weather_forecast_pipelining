import requests
import json
from kafka import KafkaProducer

#Coordinates
LAT=32.94
LON=-96.73
URL = f"https://www.7timer.info/bin/api.pl?lon={LON}&lat={LAT}&product=civil&output=json"

#Fetch Weather Data
response=requests.get(URL)
data=response.json()

# print(data)

producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send each data point to Kafka topic
for datapoint in data.get("dataseries", []):
    datapoint['location'] = {'lat': LAT, 'lon': LON}  # Include location
    producer.send("weather_raw", value=datapoint)

producer.flush()
producer.close()

print("✅ Weather data sent to Kafka topic 'weather_raw'.")