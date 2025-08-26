import json, os, time, random, datetime as dt
from kafka import KafkaProducer

KAFKA_BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
TOPIC=os.getenv('TOPIC','bet_events')

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

players=[1,2,3,4,5]
games=[10,11,12,13]
game_types={10:'sports',11:'casino',12:'poker',13:'virtual'}

def make_event():
    pid=random.choice(players)
    gid=random.choice(games)
    stake=round(random.uniform(10,300),2)
    odds=round(random.uniform(1.1,3.0),2)
    now=dt.datetime.utcnow().isoformat()+'Z'
    return {
        'event_type':'bet_placed',
        'player_id':pid,
        'game_id':gid,
        'game_type':game_types[gid],
        'stake':stake,
        'odds':odds,
        'bet_time':now
    }

if __name__=='__main__':
    print('Streaming synthetic events to', TOPIC)
    for _ in range(50):
        producer.send(TOPIC, make_event())
        time.sleep(0.2)
    producer.flush()
    print('Done.')
