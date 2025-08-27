# iGaming Data Platform Starter (Local-Only)

An end-to-end **local** demo of an iGaming data platform. No cloud spend—everything runs in Docker.
- **Postgres** (simulated transactional DB)
- **Redpanda** (Kafka-compatible) + **Redpanda Console**
- **MinIO** (S3-compatible object storage)
- **DuckDB** or **Postgres** as targets for **dbt**
- **Metabase** for BI dashboards
- **MkDocs** for docs (publish via GitHub Pages)

## Quick start
```bash
make up
make seed
make kafka
make stream
make compact   # optional
make dbt
make docs
```


> 👉 **Live docs:** https://<your-username>.github.io/igaming-demo/
