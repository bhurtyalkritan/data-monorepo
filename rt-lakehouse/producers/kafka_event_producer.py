import json
import random
import time
import uuid
import os
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

# Tuning knobs
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "300"))
BAD_EVENT_RATIO = float(os.getenv("BAD_EVENT_RATIO", "0.01"))           # fraction of events with intentional DQ issues
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.02"))           # fraction of events that are true dupes (same event_id)
LATE_EVENT_RATIO = float(os.getenv("LATE_EVENT_RATIO", "0.05"))         # fraction of events with backdated timestamps
MAX_OUT_OF_ORDER_SEC = int(os.getenv("MAX_OUT_OF_ORDER_SEC", "900"))     # late by up to this many seconds
SKEW_TOP_USERS = int(os.getenv("SKEW_TOP_USERS", "50"))                  # top N users to concentrate traffic on
SKEW_USER_RATIO = float(os.getenv("SKEW_USER_RATIO", "0.6"))            # probability an event belongs to a top user
KEY_FIELD = os.getenv("KEY_FIELD", "user_id")                             # kafka message key: user_id|event_id|country|none
INCLUDE_HEADERS = os.getenv("INCLUDE_HEADERS", "1").lower() in ("1", "true", "yes")
BURST_EVERY_SEC = int(os.getenv("BURST_EVERY_SEC", "0"))                 # 0 disables bursts; otherwise burst periodically
BURST_MULTIPLIER = float(os.getenv("BURST_MULTIPLIER", "3.0"))           # multiply EPS during burst window
BURST_DURATION_SEC = int(os.getenv("BURST_DURATION_SEC", "5"))           # duration of each burst

# Reference data
products = [f"p-{i}" for i in range(1000)]
users = [f"u-{i}" for i in range(20000)]

# Top users for skew
_top_users = users[:max(1, min(SKEW_TOP_USERS, len(users)))]

countries = ["US", "CA", "GB", "DE", "IN", "BR", "AU"]
devices = ["mobile", "desktop", "tablet"]
campaigns = ["spring_sale", "black_friday", "new_user", None, None]
referrers = ["direct", "google", "facebook", "twitter", "newsletter"]

# Multi-currency support (simple static FX map)
currencies = ["USD", "EUR", "GBP", "INR", "BRL", "AUD", "CAD"]
fx = {
    "USD": 1.0, "EUR": 0.92, "GBP": 0.78, "INR": 83.0, "BRL": 5.1, "AUD": 1.5, "CAD": 1.35
}

# Simple reservoir for duplicates
_recent_events = []
_RECENT_MAX = 2000


def _choose_user():
    if random.random() < SKEW_USER_RATIO:
        return random.choice(_top_users)
    return random.choice(users)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _maybe_late(ts: datetime) -> datetime:
    if random.random() < LATE_EVENT_RATIO and MAX_OUT_OF_ORDER_SEC > 0:
        return ts - timedelta(seconds=random.randint(1, MAX_OUT_OF_ORDER_SEC))
    return ts


def _maybe_bad_event(evt: dict) -> dict:
    if random.random() >= BAD_EVENT_RATIO:
        return evt
    # Inject one of several data quality issues
    choice = random.choice(["null_price", "neg_price", "missing_user", "bad_event_type", "qty_string", "empty_product"])
    if choice == "null_price" and evt["event_type"] in ("add_to_cart", "purchase"):
        evt["price"] = None
    elif choice == "neg_price" and evt["event_type"] == "purchase":
        evt["price"] = -abs(evt.get("price") or 0)
    elif choice == "missing_user":
        evt["user_id"] = None
    elif choice == "bad_event_type":
        evt["event_type"] = "unknown_event"
    elif choice == "qty_string" and evt["event_type"] == "purchase":
        evt["quantity"] = "two"
    elif choice == "empty_product":
        evt["product_id"] = ""
    return evt


def make_event():
    et = random.choices(["page_view", "add_to_cart", "purchase"], [0.86, 0.11, 0.03])[0]
    base_price_usd = round(random.uniform(5, 200), 2)
    qty = 1 if et != "purchase" else random.choice([1, 1, 1, 2, 3])
    country = random.choice(countries)
    cur = random.choice(currencies) if et in ("add_to_cart", "purchase") else "USD"
    # Convert to selected currency (roughly)
    price = base_price_usd if cur == "USD" else round(base_price_usd * fx.get(cur, 1.0), 2)

    user_id = _choose_user()
    session_id = f"s-{user_id}-{random.randint(1, 10000)}"
    device = random.choice(devices)

    ts_dt = _maybe_late(datetime.now(timezone.utc))

    evt = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": random.choice(products),
        "event_type": et,
        "price": price if et in ("add_to_cart", "purchase") else None,
        "quantity": qty if et == "purchase" else None,
        "currency": cur,
        "ts": ts_dt.isoformat(),
        "ua": "Mozilla/5.0",
        "country": country,
        # Extra attributes (ignored by strict Spark schema but useful downstream)
        "session_id": session_id,
        "device": device,
        "campaign": random.choice(campaigns),
        "referrer": random.choice(referrers),
    }

    evt = _maybe_bad_event(evt)

    # Track for potential duplicates
    _recent_events.append(evt)
    if len(_recent_events) > _RECENT_MAX:
        del _recent_events[0:len(_recent_events) - _RECENT_MAX]

    return evt


def _choose_key(evt: dict):
    if KEY_FIELD.lower() in ("none", "", "null"):
        return None
    key_val = evt.get(KEY_FIELD) or ""
    try:
        return str(key_val).encode("utf-8") if key_val is not None else None
    except Exception:
        return None


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
    print(f"Config -> EPS:{EVENTS_PER_SEC} bad:{BAD_EVENT_RATIO} dup:{DUPLICATE_RATIO} late:{LATE_EVENT_RATIO} skew:{SKEW_USER_RATIO}@{SKEW_TOP_USERS} bursts:{BURST_EVERY_SEC}s x{BURST_MULTIPLIER} for {BURST_DURATION_SEC}s key:{KEY_FIELD} headers:{INCLUDE_HEADERS}", flush=True)
    
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
        
        base_interval = 1.0 / EVENTS_PER_SEC if EVENTS_PER_SEC > 0 else 0
        print(f"Generating {EVENTS_PER_SEC} events per second", flush=True)
        
        def on_send_error(excp):
            print(f"Send failed: {excp}", flush=True)
        
        count = 0
        burst_until = 0
        start_time = time.time()

        while True:
            try:
                now = time.time()
                # Handle burst logic
                if BURST_EVERY_SEC > 0:
                    # start a new burst window periodically
                    if int(now - start_time) % max(BURST_EVERY_SEC,1) == 0 and burst_until < now:
                        burst_until = now + BURST_DURATION_SEC
                current_interval = base_interval
                if now < burst_until:
                    current_interval = base_interval / max(BURST_MULTIPLIER, 1.0)

                # Choose duplicate or new event
                if _recent_events and random.random() < DUPLICATE_RATIO:
                    # send a duplicate (same event_id and content)
                    event = random.choice(_recent_events)
                else:
                    event = make_event()

                key_bytes = _choose_key(event)
                headers = []
                if INCLUDE_HEADERS:
                    try:
                        headers = [
                            ("event_type", str(event.get("event_type", "")).encode("utf-8")),
                            ("country", str(event.get("country", "")).encode("utf-8")),
                            ("schema", b"v1"),
                        ]
                    except Exception:
                        headers = []

                fut = p.send(TOPIC, key=key_bytes, value=event, headers=headers if headers else None)
                fut.add_errback(on_send_error)
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} events so far...", flush=True)
                    p.flush()
                time.sleep(current_interval)
            except KeyboardInterrupt:
                print("Interrupted, flushing and exiting...", flush=True)
                break
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
        # graceful shutdown
        try:
            p.flush(10)
        except Exception:
            pass
        try:
            p.close()
        except Exception:
            pass
    except Exception as e:
        print(f"Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
