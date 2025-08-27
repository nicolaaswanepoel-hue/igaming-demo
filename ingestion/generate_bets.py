#!/usr/bin/env python3
import os, time, json, random, uuid, argparse, csv
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
TOPIC   = os.getenv("KAFKA_TOPIC", "bets")

def load_seed_ids(path):
    ids = set()
    try:
        with open(path, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                # accept either players.csv/games.csv format
                for k in ("player_id","id"):
                    if k in row and row[k]:
                        ids.add(int(row[k]))
                        break
    except FileNotFoundError:
        pass
    return sorted(ids) if ids else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=10.0, help="events per second")
    ap.add_argument("--bursts", type=int, default=0, help="send N bursts of ~rate*5 quickly")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--brokers", default=BROKERS)
    ap.add_argument("--players", default="data/seed/players.csv")
    ap.add_argument("--games", default="data/seed/games.csv")
    ap.add_argument("--duration", type=int, default=0, help="seconds to run (0 = infinite)")
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.brokers})
    players = load_seed_ids(args.players) or list(range(1, 51))
    games   = load_seed_ids(args.games)   or list(range(1, 21))

    print(f"Producing to {args.topic} @ {args.rate} eps (brokers={args.brokers})")
    t_start = time.time()
    sent = 0

    def make_event(now):
        player_id = random.choice(players)
        game_id   = random.choice(games)
        stake     = round(random.choice([5,10,20,50,100,200]) * random.uniform(0.8,1.2), 2)
        odds      = round(random.choice([1.5,1.8,2.0,2.5,3.0]) * random.uniform(0.95,1.05), 2)
        # win probability inversely related to odds (rough heuristic)
        p_win     = max(0.05, min(0.85, 1.0/odds * 0.6))
        won       = random.random() < p_win
        actual_win = round(stake * (odds if won else 0), 2)
        return {
            "bet_id": str(uuid.uuid4()),
            "player_id": player_id,
            "game_id": game_id,
            "bet_time": (now).strftime("%Y-%m-%d %H:%M:%S"),
            "stake": stake,
            "odds": odds,
            "status": "won" if won else "lost",
            "actual_win": actual_win
        }

    try:
        while True:
            now = datetime.now(timezone.utc) - timedelta(hours=random.uniform(0, 24))  # spread into last 24h
            ev = make_event(now)
            p.produce(args.topic, key=str(ev["player_id"]).encode(), value=json.dumps(ev).encode())
            sent += 1

            # bursts (optional)
            if args.bursts and sent % int(args.rate*5) == 0:
                for _ in range(int(args.rate*5)):
                    evb = make_event(now + timedelta(seconds=random.randint(1,60)))
                    p.produce(args.topic, key=str(evb["player_id"]).encode(), value=json.dumps(evb).encode())
                p.flush()

            # pacing
            time.sleep(max(0, 1.0/args.rate))

            if args.duration and time.time() - t_start >= args.duration:
                break
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()
        print(f"Produced {sent} events")
if __name__ == "__main__":
    main()
