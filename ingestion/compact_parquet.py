import os, json, io
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

endpoint   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
secure     = bool(int(os.getenv("MINIO_SECURE", "0")))
bucket     = os.getenv("MINIO_BUCKET", "lake")

m = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

objects = m.list_objects(bucket, prefix="bets/", recursive=True)

rows = []
for obj in objects:
    resp = m.get_object(bucket, obj.object_name)
    data = resp.read()
    resp.close()
    resp.release_conn()
    rows.append(json.loads(data.decode("utf-8")))

# convert to Arrow table
table = pa.Table.from_pylist(rows)
buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

parquet_name = "bets_compacted/bets.parquet"
m.put_object(bucket, parquet_name, buf, length=len(buf.getvalue()), content_type="application/octet-stream")
print(f"Compacted {len(rows)} messages into {parquet_name}")
