
### `docs/quickstart.md`
```md
# Quickstart

```bash
# 1) bring up infra
docker compose up -d

# 2) (one liner) generate → ingest → mirror → compact
./ingestion/kickoff_stream.sh
# knobs: RATE=50 DURATION=300 ./ingestion/kickoff_stream.sh

# 3) build models
cd transformations/dbt_project
export DBT_PROFILES_DIR=$PWD
dbt run

# 4) open Metabase: http://localhost:3000
#    connect Postgres (host: postgres, user: igaming, pass: example, db: igaming)