#!/usr/bin/env python3
import os, json, io, time
from datetime import datetime, timezone
from confluent_kafka import Consumer
from minio import Minio

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
TOPIC   = os.getenv("KAFKA_TOPIC", "bets")
BATCH_SIZE = int(os.getenv("MINIO_BATCH_SIZE", "1000"))   # messages per file
BATCH_SEC  = int(os.getenv("MINIO_BATCH_SEC",  "30"))     # seconds per file max

endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
secure     = bool(int(os.getenv("MINIO_SECURE", "0")))
bucket     = os.getenv("MINIO_BUCKET", "lake")

def put_object(m, path, lines):
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    m.put_object(bucket, path, data=io.BytesIO(payload), length=len(payload),
                 content_type="application/x-ndjson")
    print(f"-> {path} ({len(lines)} msgs)")

def main():
    c = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": "minio-mirror-jsonl",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    })
    c.subscribe([TOPIC])

    m = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not m.bucket_exists(bucket):
        m.make_bucket(bucket)

    lines = []
    window_start = time.time()

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                # time-based flush
                if lines and (time.time() - window_start) >= BATCH_SEC:
                    now = datetime.now(timezone.utc)
                    day, hour = now.strftime("%Y-%m-%d"), now.strftime("%H")
                    ts = now.strftime("%Y%m%dT%H%M%S")
                    path = f"{TOPIC}/dt={day}/hour={hour}/part-{ts}.jsonl"
                    put_object(m, path, lines)
                    lines, window_start = [], time.time()
                continue

            if msg.error():
                print("Kafka error:", msg.error()); 
                continue

            # keep line as raw JSON string (no reformat)
            try:
                # ensure it's valid JSON; then store the original text
                _ = json.loads(msg.value())
                lines.append(msg.value().decode("utf-8"))
            except Exception:
                # skip or wrap invalid messages
                continue

            # size-based flush
            if len(lines) >= BATCH_SIZE:
                now = datetime.now(timezone.utc)
                day, hour = now.strftime("%Y-%m-%d"), now.strftime("%H")
                ts = now.strftime("%Y%m%dT%H%M%S")
                path = f"{TOPIC}/dt={day}/hour={hour}/part-{ts}.jsonl"
                put_object(m, path, lines)
                lines, window_start = [], time.time()

    except KeyboardInterrupt:
        print("Stopping mirror...")
        if lines:
            now = datetime.now(timezone.utc)
            day, hour = now.strftime("%Y-%m-%d"), now.strftime("%H")
            ts = now.strftime("%Y%m%dT%H%M%S")
            path = f"{TOPIC}/dt={day}/hour={hour}/part-{ts}-final.jsonl"
            put_object(m, path, lines)
    finally:
        c.close()

if __name__ == "__main__":
    main()
