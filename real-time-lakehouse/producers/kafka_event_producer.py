import json, random, time, uuid, os
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

products = [f"p-{i}" for i in range(1000)]
users    = [f"u-{i}" for i in range(20000)]
countries= ["US","CA","GB","DE","IN","BR","AU"]

def make_event():
    et = random.choices(["page_view","add_to_cart","purchase"], [0.86,0.11,0.03])[0]
    price = round(random.uniform(5,200),2)
    qty = 1 if et!="purchase" else random.choice([1,1,1,2,3])
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "event_type": et,
        "price": price if et in ("add_to_cart","purchase") else None,
        "quantity": qty if et=="purchase" else None,
        "currency": "USD",
        "ts": datetime.now(timezone.utc).isoformat(),
        "ua": "Mozilla/5.0",
        "country": random.choice(countries),
    }

if __name__ == "__main__":
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                      value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    rate = float(os.getenv("EVENTS_PER_SEC", "300"))
    interval = 1.0 / rate
    while True:
        p.send(TOPIC, value=make_event())
        time.sleep(interval)
