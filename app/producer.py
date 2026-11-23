"""Generates fake e-commerce events into Kafka at a steady rate.

A small share of messages are deliberately broken (to exercise the dead-letter
topic) or stamped in the past (late data landing in older day partitions).
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s producer: %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "events")
RATE = float(os.environ.get("EVENTS_PER_SECOND", "50"))
BAD_RATE = float(os.environ.get("BAD_MESSAGE_RATE", "0.01"))
LATE_RATE = float(os.environ.get("LATE_EVENT_RATE", "0.05"))

EVENT_WEIGHTS = {"page_view": 0.80, "add_to_cart": 0.12, "purchase": 0.07, "refund": 0.01}

running = True


def _stop(*_):
    global running
    running = False


def make_event() -> dict:
    now = datetime.now(UTC)
    if random.random() < LATE_RATE:
        now -= timedelta(seconds=random.randint(60, 2 * 24 * 3600))
    event_type = random.choices(list(EVENT_WEIGHTS), weights=list(EVENT_WEIGHTS.values()))[0]
    amount = round(random.uniform(5, 300), 2) if event_type in ("purchase", "refund") else None
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 5_000),
        "event_type": event_type,
        "amount": amount,
        "event_ts": now.isoformat(),
    }


def make_bad_message() -> bytes:
    return random.choice(
        [
            b"{not json",
            json.dumps({"event_id": str(uuid.uuid4()), "event_type": "page_view"}).encode(),  # missing fields
            json.dumps({**make_event(), "event_type": "teleport"}).encode(),  # unknown type
            json.dumps({**make_event(), "user_id": "abc"}).encode(),  # wrong type
        ]
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,  # no duplicates from producer retries
            "linger.ms": 20,
            "compression.type": "zstd",
        }
    )

    sent = 0
    interval = 1.0 / RATE if RATE > 0 else 0
    last_log = time.monotonic()
    log.info("producing to %s/%s at ~%.0f events/s", BOOTSTRAP, TOPIC, RATE)

    while running:
        if random.random() < BAD_RATE:
            key, value = None, make_bad_message()
        else:
            event = make_event()
            key, value = str(event["user_id"]).encode(), json.dumps(event).encode()

        while True:
            try:
                producer.produce(TOPIC, key=key, value=value)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: wait for deliveries

        producer.poll(0)
        sent += 1
        if time.monotonic() - last_log >= 10:
            log.info("sent %d messages", sent)
            last_log = time.monotonic()
        if interval:
            time.sleep(interval)

    log.info("flushing...")
    producer.flush(10)
    log.info("stopped after %d messages", sent)


if __name__ == "__main__":
    main()
