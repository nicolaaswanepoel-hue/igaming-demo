#!/usr/bin/env python3
import os, json, io
from datetime import datetime, timezone
from confluent_kafka import Consumer
from minio import Minio

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
TOPIC   = os.getenv("KAFKA_TOPIC", "bets")

endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
secure     = bool(int(os.getenv("MINIO_SECURE", "0")))
bucket     = os.getenv("MINIO_BUCKET", "lake")

def main():
    c = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": "minio-mirror",          # <- different from pg-writer
        "auto.offset.reset": "latest",       # start from new data
        "enable.auto.commit": True
    })
    c.subscribe([TOPIC])

    m = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not m.bucket_exists(bucket):
        m.make_bucket(bucket)

    print(f"Mirroring topic={TOPIC} -> s3://{bucket}/{TOPIC}/dt=YYYY-MM-DD/hour=HH")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print("Kafka error:", msg.error()); 
                continue

            val = msg.value()
            if val is None:
                continue

            now = datetime.now(timezone.utc)
            day = now.strftime("%Y-%m-%d")
            hour = now.strftime("%H")
            ts = now.strftime("%Y%m%dT%H%M%S%f")
            key = msg.key().decode("utf-8") if msg.key() else "nokey"
            obj_name = f"{TOPIC}/dt={day}/hour={hour}/{ts}_{key}.json"

            bio = io.BytesIO(val)
            m.put_object(bucket, obj_name, data=bio, length=len(val), content_type="application/json")
            print("->", obj_name)
    except KeyboardInterrupt:
        print("Stopping mirror...")
    finally:
        c.close()

if __name__ == "__main__":
    main()
