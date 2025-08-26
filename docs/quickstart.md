# Quickstart
```bash
docker compose up -d
python3 ingestion/load_sql.py
python3 ingestion/push_kafka.py
python3 ingestion/stream_to_minio.py   # keep running for streaming demo
python3 ingestion/compact_parquet.py   # optional compaction
cd transformations/dbt_project && dbt deps && dbt seed && dbt run && dbt test
```
