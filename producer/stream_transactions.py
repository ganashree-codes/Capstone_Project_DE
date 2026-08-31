import pandas as pd
import json
import time
from kafka import KafkaProducer

# Load already-cleaned, already-renamed data (prepared in analysis/eda.ipynb)
df = pd.read_csv("C:/Final_Capstone_Project/producer/data/fraudTrain_clean.csv")
df = df.sample(n=20000, random_state=42).reset_index(drop=True)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

for i, row in df.iterrows():
    event = row.to_dict()
    producer.send('raw.transactions', key=str(event['cc_num']).encode('utf-8'), value=event)

    if i % 500 == 0:
        print(f"Sent {i} events so far...")

    time.sleep(0.05)

producer.flush()
producer.close()
print(f"Done. Streamed {len(df)} events.")