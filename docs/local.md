# Local-Only Demo

## Services
- **Postgres**: `localhost:5432`
- **Redpanda Console**: `http://localhost:8080`
- **MinIO**: `http://localhost:9001`
- **Metabase**: `http://localhost:3000`

## Flow
1) Seed SQL (`players`, `games`, `bets`).
2) Stream synthetic bet events to Kafka.
3) Consume Kafka → write Parquet to MinIO partitioned by date/game_type.
4) (Optional) Compact small files using DuckDB.
5) Transform with dbt (DuckDB default) and explore via Metabase.
