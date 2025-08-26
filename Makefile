.PHONY: up seed kafka stream compact dbt docs

up:
	docker compose up -d

seed:
	python3 ingestion/load_sql.py

kafka:
	python3 ingestion/push_kafka.py

stream:
	python3 ingestion/stream_to_minio.py

compact:
	python3 ingestion/compact_parquet.py

dbt:
	cd transformations/dbt_project && dbt deps && dbt seed && dbt run && dbt test

docs:
	mkdocs serve
