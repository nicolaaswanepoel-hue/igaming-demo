import json, os, time, io, datetime as dt
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

KAFKA_BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
TOPIC=os.getenv('TOPIC','bet_events')

client = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
if not client.bucket_exists('raw'):
    client.make_bucket('raw')

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

buffer=[]
BATCH=50

def write_record(r):
    d = (r.get('bet_time') or '')[:10] or dt.date.today().isoformat()
    gt = r.get('game_type','unknown')
    key = f"events/bet_placed/event_date={d}/game_type={gt}/{int(time.time()*1000)}.parquet"
    table = pa.Table.from_pylist([r])
    sink = io.BytesIO(); pq.write_table(table, sink)
    data = sink.getvalue()
    client.put_object('raw', key, io.BytesIO(data), len(data))

print("Consuming and writing Parquet to s3://raw/events/... (Ctrl+C to stop)")
try:
    for msg in consumer:
        write_record(msg.value)
except KeyboardInterrupt:
    print("Stopped.")
