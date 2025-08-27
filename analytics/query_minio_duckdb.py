#!/usr/bin/env python3
import os
import sys
import duckdb

# ---- Config (env with sensible defaults) ----
S3_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
S3_URL_STYLE  = "path"
S3_USE_SSL    = os.getenv("MINIO_SECURE", "0") not in ("0", "false", "False", "")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET        = os.getenv("MINIO_BUCKET", "lake")
OBJ_PATH      = os.getenv("PARQUET_PATH", "bets_compacted/bets.parquet")  # overrideable

S3_URI = f"s3://{BUCKET}/{OBJ_PATH}"

def run():
    con = duckdb.connect()
    # Enable S3/HTTP
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_url_style='{S3_URL_STYLE}';")
    con.execute(f"SET s3_use_ssl={'true' if S3_USE_SSL else 'false'};")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")

    print(f"\n🔗 Reading: {S3_URI}")
    # Create a view so we can introspect schema once
    con.execute(f"CREATE OR REPLACE VIEW v_bets AS SELECT * FROM parquet_scan('{S3_URI}');")

    # What columns do we have?
    cols = set(r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns() WHERE table_name='v_bets'
    """).fetchall())
    print(f"🧾 Columns: {', '.join(sorted(cols))}\n")

    def safe_query(title, sql):
        print(f"=== {title} ===")
        try:
            df = con.execute(sql).fetchdf()
            if df.empty:
                print("(no rows)\n")
            else:
                # Pretty print without extra deps
                print(df.to_string(index=False))
                print()
        except Exception as e:
            print(f"(skipped: {e})\n")

    # 1) Top games by bet count
    if {"game_id"} <= cols:
        safe_query(
            "Top games by bet count",
            "SELECT game_id, COUNT(*) AS bets "
            "FROM v_bets GROUP BY 1 ORDER BY bets DESC LIMIT 10;"
        )

    # 2) Daily bets & outcomes (requires placed_at & status)
    if {"placed_at", "status"} <= cols:
        safe_query(
            "Daily bets & outcomes",
            "SELECT date_trunc('day', CAST(placed_at AS TIMESTAMP)) AS day, "
            "       COUNT(*) AS bets, "
            "       SUM(CASE WHEN status='won'  THEN 1 ELSE 0 END) AS wins, "
            "       SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS losses "
            "FROM v_bets GROUP BY 1 ORDER BY 1;"
        )

    # 3) Win rate by game (status + game_id)
    if {"game_id", "status"} <= cols:
        safe_query(
            "Win rate by game",
            "SELECT game_id, "
            "       AVG(CASE WHEN status='won' THEN 1.0 ELSE 0.0 END) AS win_rate, "
            "       COUNT(*) AS bets "
            "FROM v_bets GROUP BY 1 HAVING COUNT(*) >= 1 "
            "ORDER BY win_rate DESC, bets DESC LIMIT 10;"
        )

    # 4) Handle & GGR if amounts exist (stake/amount & payout/return)
    # common names people use: stake, amount, wager, wager_amount, payout, win_amount, returned
    amount_cols = [c for c in cols if c.lower() in {"stake","amount","wager","wager_amount"}]
    payout_cols = [c for c in cols if c.lower() in {"payout","win_amount","returned","return"}]
    if amount_cols:
        amt = amount_cols[0]
        if "placed_at" in cols:
            safe_query(
                f"Daily handle (sum of {amt})",
                f"SELECT date_trunc('day', CAST(placed_at AS TIMESTAMP)) AS day, "
                f"       SUM(TRY_CAST({amt} AS DOUBLE)) AS handle "
                f"FROM v_bets GROUP BY 1 ORDER BY 1;"
            )
        if payout_cols:
            pay = payout_cols[0]
            safe_query(
                f"GGR by day using {amt} & {pay}",
                f"SELECT date_trunc('day', CAST(placed_at AS TIMESTAMP)) AS day, "
                f"       SUM(TRY_CAST({amt} AS DOUBLE) - TRY_CAST({pay} AS DOUBLE)) AS ggr "
                f"FROM v_bets GROUP BY 1 ORDER BY 1;"
            )

if __name__ == "__main__":
    # Allow optional path override: python analytics/query_minio_duckdb.py bets_compacted/alt.parquet
    if len(sys.argv) > 1:
        os.environ["PARQUET_PATH"] = sys.argv[1]
    run()
