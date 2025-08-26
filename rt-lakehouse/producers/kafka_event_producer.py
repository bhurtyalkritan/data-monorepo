import json, random, time, uuid, os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

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

def wait_for_kafka(bootstrap_servers, max_retries=30):
    """Wait for Kafka to be available with exponential backoff"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...", flush=True)
            # Try to create a producer with a short timeout
            test_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            test_producer.close()
            print("Successfully connected to Kafka!", flush=True)
            return True
        except Exception as e:
            wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30 seconds
            print(f"Failed to connect to Kafka: {e}", flush=True)
            print(f"Retrying in {wait_time} seconds...", flush=True)
            time.sleep(wait_time)
    
    print(f"Failed to connect to Kafka after {max_retries} attempts", flush=True)
    return False

if __name__ == "__main__":
    print(f"Starting producer with bootstrap servers: {BOOTSTRAP}", flush=True)
    print(f"Topic: {TOPIC}", flush=True)
    
    # Wait for Kafka to be ready
    if not wait_for_kafka(BOOTSTRAP):
        print("Exiting due to Kafka connection failure", flush=True)
        exit(1)
    
    try:
        p = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='1',
            linger_ms=50,
            request_timeout_ms=15000,
            retries=5,
            retry_backoff_ms=500,
        )
        print("Kafka producer initialized successfully", flush=True)
        
        rate = float(os.getenv("EVENTS_PER_SEC", "300"))
        interval = 1.0 / rate
        print(f"Generating {rate} events per second", flush=True)
        
        def on_send_success(record_metadata):
            # Print occasionally to avoid log spam
            pass
        
        def on_send_error(excp):
            print(f"Send failed: {excp}", flush=True)
        
        count = 0
        while True:
            try:
                event = make_event()
                fut = p.send(TOPIC, value=event)
                fut.add_errback(on_send_error)
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} events so far...", flush=True)
                    p.flush()
                time.sleep(interval)
            except Exception as e:
                print(f"Error sending event: {e}", flush=True)
                # Try to reconnect
                print("Attempting to reconnect to Kafka...", flush=True)
                try:
                    p.close()
                except Exception:
                    pass
                if wait_for_kafka(BOOTSTRAP, max_retries=5):
                    p = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                                      value_serializer=lambda v: json.dumps(v).encode("utf-8"))
                    print("Reconnected successfully", flush=True)
                else:
                    print("Failed to reconnect, exiting", flush=True)
                    break
    except Exception as e:
        print(f"Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
