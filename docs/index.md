# iGaming Demo

An end-to-end, **streaming data platform**:

- 🧪 **Generator** → Synthetic bets into Kafka (Redpanda)
- 🗄️ **Warehouse** → Postgres consumer (real-time append)
- 🪣 **Lake** → MinIO mirror + JSONL → Parquet compaction
- 🧱 **dbt** → `stg_bets`, `fact_bet`, `daily_game_metrics`
- 📊 **Metabase** → dashboards on top

## Quick links
- Repo: [GitHub]({{ config.repo_url }})
- Start everything: `docker compose up -d`
- One-shot pipeline: `./ingestion/kickoff_stream.sh`

!!! tip "What this showcases"
    - Real-time ingestion + dual sinks (warehouse & lake)
    - Lakehouse compaction (JSONL → Parquet)
    - Reusable metrics via dbt
    - BI exploration in Metabase
