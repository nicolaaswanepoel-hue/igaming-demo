#!/usr/bin/env bash
set -euo pipefail

# Load env if present (PG, Kafka, MinIO, etc.)
source .env.wsl 2>/dev/null || true

# Tunables (can be overridden via env when calling the script)
: "${RATE:=20}"                  # events/sec
: "${DURATION:=120}"             # seconds to run the generator
: "${MINIO_BATCH_SIZE:=1000}"    # messages per JSONL object
: "${MINIO_BATCH_SEC:=30}"       # seconds per JSONL object
: "${KAFKA_TOPIC:=bets}"
: "${KAFKA_BROKERS:=localhost:19092}"   # use 9092 if you used /etc/hosts redpanda->localhost

# Keep track of background PIDs and clean up on exit
PIDS=()
cleanup() {
  echo "Stopping background consumers..."
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

echo "▶ Starting Postgres consumer..."
python ingestion/kafka_to_postgres.py &
PIDS+=($!)

echo "▶ Starting MinIO mirror (JSONL)…"
MINIO_BATCH_SIZE="$MINIO_BATCH_SIZE" MINIO_BATCH_SEC="$MINIO_BATCH_SEC" \
python ingestion/kafka_to_minio_jsonl.py &
PIDS+=($!)

# tiny warm-up so consumers subscribe before we produce
sleep 2

echo "▶ Generating bets to Kafka topic '$KAFKA_TOPIC' @ ${RATE} eps for ${DURATION}s..."
python ingestion/generate_bets.py \
  --rate "$RATE" \
  --duration "$DURATION" \
  --topic "$KAFKA_TOPIC" \
  --brokers "$KAFKA_BROKERS"

echo "✔ Generation finished. Waiting a moment for consumers to flush..."
sleep 3

# Compact today's partition to Parquet
TODAY=$(date +%F)
echo "▶ Compacting MinIO objects for date ${TODAY} → Parquet…"
DATE="$TODAY" python ingestion/compact_parquet.py

echo "✅ All done: generator, DB ingest, MinIO mirror, and Parquet compaction."
# background consumers will be stopped by trap/cleanup
