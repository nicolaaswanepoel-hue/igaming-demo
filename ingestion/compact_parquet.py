# ingestion/compact_parquet.py (partition-aware)
import os, json, io
import pyarrow as pa, pyarrow.parquet as pq
from minio import Minio

endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
secure     = bool(int(os.getenv("MINIO_SECURE", "0")))
bucket     = os.getenv("MINIO_BUCKET", "lake")
topic      = os.getenv("KAFKA_TOPIC", "bets")
date_part  = os.getenv("DATE", "")  # e.g. 2025-08-27; if blank, compacts everything (careful)

m = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

prefix = f"{topic}/"
if date_part:
    prefix += f"dt={date_part}/"

rows=[]
for obj in m.list_objects(bucket, prefix=prefix, recursive=True):
    if not obj.object_name.endswith((".json",".jsonl")): 
        continue
    resp = m.get_object(bucket, obj.object_name)
    data = resp.read().decode("utf-8", errors="ignore")
    resp.close(); resp.release_conn()
    # handle json or jsonl
    for line in (data.splitlines() if obj.object_name.endswith(".jsonl") else [data]):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

if not rows:
    print("No rows found for", prefix); raise SystemExit(0)

table = pa.Table.from_pylist(rows)
buf = io.BytesIO(); pq.write_table(table, buf); buf.seek(0)

out_key = f"{topic}_parquet/"
if date_part: out_key += f"dt={date_part}/"
out_key += "part-000.parquet"

m.put_object(bucket, out_key, data=buf, length=len(buf.getvalue()), content_type="application/octet-stream")
print(f"Compacted {len(rows)} rows -> s3://{bucket}/{out_key}")
