import os, json, io
from confluent_kafka import Consumer
from minio import Minio
from datetime import datetime, timezone

# Kafka config
brokers = os.getenv("KAFKA_BROKERS", "localhost:19092")  # use 19092 if you set dual listeners
topic   = os.getenv("KAFKA_TOPIC", "bets")

# MinIO config
endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
secure     = bool(int(os.getenv("MINIO_SECURE", "0")))
bucket     = os.getenv("MINIO_BUCKET", "lake")

# init clients
c = Consumer({
    'bootstrap.servers': brokers,
    'group.id': 'minio-writer',
    'auto.offset.reset': 'earliest'
})
c.subscribe([topic])

m = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
if not m.bucket_exists(bucket):
    m.make_bucket(bucket)

print(f"Consuming from topic={topic}, writing to bucket={bucket}... Ctrl+C to stop.")
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue
        key = msg.key().decode('utf-8') if msg.key() else "nokey"
        value = msg.value().decode('utf-8')  # already JSON from your producer
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        object_name = f"{topic}/{ts}_{key}.json"
        bio = io.BytesIO(value.encode("utf-8"))
        m.put_object(
            bucket,
            object_name,
            data=bio,
            length=len(value),
            content_type="application/json",
        )
        print(f"Stored {object_name}")
except KeyboardInterrupt:
    print("Stopping...")
finally:
    c.close()
