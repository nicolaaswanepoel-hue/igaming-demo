#!/usr/bin/env python3
import os, json, signal, psycopg2, psycopg2.extras
from confluent_kafka import Consumer

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
TOPIC   = os.getenv("KAFKA_TOPIC", "bets")

PGHOST=os.getenv("PGHOST","localhost")
PGPORT=int(os.getenv("PGPORT","5432"))
PGUSER=os.getenv("PGUSER","igaming")
PGPASSWORD=os.getenv("PGPASSWORD","example")
PGDATABASE=os.getenv("PGDATABASE","igaming")

BATCH=500
RUN=True
def stop(*_): 
    global RUN; RUN=False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

def nz(v):
    """normalize: '' -> None"""
    if v is None: return None
    if isinstance(v, str) and v.strip()=='':
        return None
    return v

def to_int(v):
    v = nz(v)
    try: return None if v is None else int(v)
    except: return None

def to_float(v):
    v = nz(v)
    try: return None if v is None else float(v)
    except: return None

def main():
    c = Consumer({
      "bootstrap.servers": BROKERS,
      "group.id": "pg-writer",
      "auto.offset.reset": "latest",   # start from new events
      "enable.auto.commit": False
    })
    c.subscribe([TOPIC])

    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS public.bets_stream(
        bet_id      text PRIMARY KEY,
        player_id   integer,
        game_id     integer,
        bet_time    timestamp,
        stake       double precision,
        odds        double precision,
        status      text,
        actual_win  double precision
      );
    """)
    conn.commit()

    print(f"Consuming {TOPIC} → Postgres {PGHOST}:{PGPORT}/{PGDATABASE}. Ctrl+C to stop.")
    pending=[]
    try:
        while RUN:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print("Kafka error:", msg.error()); 
                continue

            try:
                row = json.loads(msg.value())
                pending.append((
                    nz(row.get("bet_id")),
                    to_int(row.get("player_id")),
                    to_int(row.get("game_id")),
                    nz(row.get("bet_time")),   # ISO 'YYYY-MM-DD HH:MM:SS' works; Z also ok if pg understands it
                    to_float(row.get("stake")),
                    to_float(row.get("odds")),
                    nz(row.get("status")),
                    to_float(row.get("actual_win")),
                ))
            except Exception as e:
                print("Bad message, skipping:", e)

            if len(pending) >= BATCH:
                psycopg2.extras.execute_values(cur, """
                  INSERT INTO public.bets_stream
                  (bet_id, player_id, game_id, bet_time, stake, odds, status, actual_win)
                  VALUES %s
                  ON CONFLICT (bet_id) DO NOTHING
                """, pending, page_size=BATCH)
                conn.commit()
                c.commit()
                print(f"Inserted {len(pending)} rows")
                pending.clear()

        if pending:
            psycopg2.extras.execute_values(cur, """
              INSERT INTO public.bets_stream
              (bet_id, player_id, game_id, bet_time, stake, odds, status, actual_win)
              VALUES %s
              ON CONFLICT (bet_id) DO NOTHING
            """, pending, page_size=BATCH)
            conn.commit()
            c.commit()
            print(f"Inserted {len(pending)} rows (final flush)")
    finally:
        cur.close(); conn.close(); c.close()

if __name__ == "__main__":
    main()
