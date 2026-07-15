import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("notify")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")


def format_notification(event: dict) -> str:
    """Pure: render the user-facing notification for a done event."""
    return (
        f"🔔 [notify] Hey {event['user']} — your job "
        f"{event['job_id']} ({event['action']}) is {event['status']}!"
    )


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "notifiers",
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

    log.info("notifier up — consuming %s, group=notifiers", EVENTS_TOPIC)
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
            log.info(format_notification(event))  # real system: email/SMS/push
        except Exception:
            log.exception("failed to handle event; skipping")

    consumer.close()
    log.info("notifier shutting down")


if __name__ == "__main__":
    main()
