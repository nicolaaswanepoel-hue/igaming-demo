#!/usr/bin/env bash
set -euo pipefail
source .env.wsl 2>/dev/null || true
# start consumer in background
python ingestion/kafka_to_postgres.py &
CONS_PID=$!
# generate at 20 eps for 120 seconds
python ingestion/generate_bets.py --rate 20 --duration 120
wait $CONS_PID || true
