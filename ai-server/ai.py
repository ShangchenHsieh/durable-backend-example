import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("ai")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")


def enrich(event: dict) -> str:
    """Pure: simulate an AI-generated summary for a completed job."""
    return (
        f"🤖 [ai] summary for job {event['job_id']}: "
        f"{event['user']}'s '{event['action']}' completed successfully"
    )


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "ai",
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

    log.info("ai up — consuming %s, group=ai", EVENTS_TOPIC)
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
            log.info(enrich(event))  # real system: call an LLM / ML model
        except Exception:
            log.exception("failed to handle event; skipping")

    consumer.close()
    log.info("ai shutting down")


if __name__ == "__main__":
    main()
