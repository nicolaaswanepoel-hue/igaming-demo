# Architecture

```mermaid
flowchart LR
  A[Generator\n(generate_bets.py)] -->|JSON events| B((Redpanda\nKafka))
  B --> C[Consumer → Postgres\nkafka_to_postgres.py]
  B --> D[Mirror → MinIO (JSON/JSONL)\nkafka_to_minio*.py]
  D --> E[Compaction → Parquet\ncompact_parquet.py]
  C --> F[dbt models\nstg_bets, fact_bet, daily_game_metrics]
  F --> G[Metabase]
  E --> H[DuckDB (ad-hoc)]
