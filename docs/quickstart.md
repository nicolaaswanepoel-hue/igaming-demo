# Quickstart

Follow these steps to run the full demo locally in a few minutes.

## 1) Bring up infrastructure

```bash
docker compose up -d
docker compose ps
```

## 2) Run the end-to-end pipeline (one command)

```bash
defaults: RATE=20 eps, DURATION=120s
./ingestion/kickoff_stream.sh

# or tune knobs:
RATE=50 DURATION=300 ./ingestion/kickoff_stream.sh
```
This will:

```
start the Postgres consumer
start the MinIO mirror (JSONL)
generate synthetic bets into Kafka
compact today’s objects to Parquet in MinIO
```

## 3) Build models with dbt
```
cd transformations/dbt_project
export DBT_PROFILES_DIR=$PWD
dbt run
```

## 4) Explore in Metabase
```
Open http://localhost:3000
 and connect Postgres:

Host:     postgres
Port:     5432
DB name:  igaming
User:     igaming
Password: example
```

Recommended starting points:

```
daily_game_metrics → line chart of day vs bets
fact_bet → bar chart of game_id vs stake
filter by status = won / lost
```