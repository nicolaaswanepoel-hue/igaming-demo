# FAQ

**Why does the Kafka client try to connect to `redpanda:9092`?**  
Use dual listeners in Docker and point external clients to `localhost:19092`, or map `redpanda` → `127.0.0.1` in `/etc/hosts`.

**Metabase shows no tables.**  
Grant schema/table permissions to the Metabase user, then Admin → Databases → Sync + Re-scan.

**dbt adapter mismatch.**  
Pin `dbt-core` and adapter to the same version (e.g., `1.9.0` / `dbt-postgres==1.9.0`).
