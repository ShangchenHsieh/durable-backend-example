import json
import logging
import os
import signal
from collections import Counter

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("analytics")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")


def update_tally(tally: Counter, event: dict) -> Counter:
    """Pure: fold one event into the running per-action tally."""
    tally[event["action"]] += 1
    return tally


def format_stats(tally: Counter) -> str:
    """Pure: render the running analytics line."""
    total = sum(tally.values())
    breakdown = ", ".join(f"{action}={n}" for action, n in sorted(tally.items()))
    return f"📊 [analytics] {total} events — {breakdown}"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "analytics",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([EVENTS_TOPIC])

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    tally = Counter()
    log.info("analytics up — consuming %s, group=analytics", EVENTS_TOPIC)
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                # Topic is auto-created on first produce; just wait for it.
                continue
            log.error("consume error: %s", msg.error())
            continue
        try:
            event = json.loads(msg.value())
            update_tally(tally, event)
            log.info(format_stats(tally))
        except Exception:
            log.exception("failed to handle event; skipping")

    consumer.close()
    log.info("analytics shutting down")


if __name__ == "__main__":
    main()
