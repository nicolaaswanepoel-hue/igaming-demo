import duckdb
duckdb.sql("""SET s3_endpoint='localhost:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='minio';
SET s3_secret_access_key='minio123';
""")
duckdb.sql("""
CREATE OR REPLACE TABLE compacted AS
SELECT * FROM read_parquet('s3://raw/events/bet_placed/**/**/*.parquet');
""")
duckdb.sql("""
COPY (SELECT * FROM compacted) TO 's3://raw/events_compacted/bet_placed.parquet' (FORMAT PARQUET);
""")
print("Compacted to s3://raw/events_compacted/bet_placed.parquet")
