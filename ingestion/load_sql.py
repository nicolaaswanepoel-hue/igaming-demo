import csv, os, psycopg2

PG_DSN = os.getenv("PG_DSN", "dbname=igaming user=igaming password=example host=localhost port=5432")

def main():
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS players(
      player_id INT PRIMARY KEY,
      registration_date DATE,
      segment TEXT,
      vip_tier TEXT,
      country TEXT
    );
    CREATE TABLE IF NOT EXISTS games(
      game_id INT PRIMARY KEY,
      game_type TEXT,
      provider TEXT,
      league TEXT,
      market TEXT
    );
    CREATE TABLE IF NOT EXISTS bets(
      bet_id INT PRIMARY KEY,
      player_id INT,
      game_id INT,
      stake NUMERIC(18,2),
      odds NUMERIC(10,2),
      bet_time TIMESTAMPTZ,
      status TEXT,
      actual_win NUMERIC(18,2)
    );
    """)

    def load_csv(path, table, cols):
        with open(path) as f:
            r = csv.DictReader(f)
            for row in r:
                values = [row[c] if row[c] != '' else None for c in cols]
                placeholders = ','.join(['%s']*len(cols))
                cur.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", values)

    base = os.path.dirname(os.path.dirname(__file__))
    load_csv(os.path.join(base, 'data/seed/players.csv'), 'players',
             ['player_id','registration_date','segment','vip_tier','country'])
    load_csv(os.path.join(base, 'data/seed/games.csv'), 'games',
             ['game_id','game_type','provider','league','market'])
    load_csv(os.path.join(base, 'data/seed/bets.csv'), 'bets',
             ['bet_id','player_id','game_id','stake','odds','bet_time','status','actual_win'])

    conn.commit()
    print("Loaded seed data into Postgres.")

if __name__ == '__main__':
    main()
