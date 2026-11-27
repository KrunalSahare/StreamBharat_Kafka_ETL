"""Kafka output for the live feed (producers/live.py). You should not need to change this file."""
import json, os
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    """Indian time, whatever the container's clock zone is (timestamps carry no zone, like the source systems)."""
    return datetime.now(IST).replace(tzinfo=None)


def add_args(ap):
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
    ap.add_argument("--dry-run", action="store_true", help="write to _dryrun/ instead of Kafka")


def make_write(args, topics, keys, client_id):
    """topics: feed name -> topic; keys: feed name -> field used as the message key.
    Returns write(folder, rows, stamp) with the same call shape as the file-based simulator."""
    if args.dry_run:
        os.makedirs("_dryrun", exist_ok=True)
        files = {}

        def send(topic, key, row):
            f = files.setdefault(topic, open(f"_dryrun/{topic}.jsonl", "a", encoding="utf-8"))
            f.write(json.dumps({"key": key, "value": row}) + "\n")
            f.flush()
    else:
        from confluent_kafka import Producer
        prod = Producer({"bootstrap.servers": args.bootstrap, "client.id": client_id,
                         "enable.idempotence": True, "acks": "all", "linger.ms": 10})

        def send(topic, key, row):
            while True:
                try:
                    prod.produce(topic, key=None if key is None else str(key).encode(), value=json.dumps(row).encode())
                    break
                except BufferError:
                    prod.poll(0.5)
            prod.poll(0)

    def write(folder, rows, stamp=None):
        feed = os.path.basename(folder.rstrip("/"))
        for r in rows:
            send(topics[feed], r.get(keys[feed]), r)
        if not args.dry_run:
            prod.flush(5)

    return write
