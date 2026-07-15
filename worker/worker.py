import json
import logging
import os
import signal
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("worker")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
JOBS_TOPIC = os.getenv("JOBS_TOPIC", "jobs")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")
WORK_SECONDS = float(os.getenv("WORK_SECONDS", "5"))
# Liveness heartbeat: the K8s exec probe restarts us if this file goes stale
# (see k8s/40-worker.yaml). Harmless under docker-compose — nothing reads it there.
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/worker-alive"))


def heartbeat(path: Path = HEARTBEAT_FILE) -> None:
    """Touch the liveness heartbeat file; its mtime is 'last time the loop ran'."""
    path.touch()


def build_event(payload: dict) -> dict:
    """Pure: turn a completed job into its 'done' event."""
    return {
        "job_id": payload["job_id"],
        "user": payload["user"],
        "action": payload["action"],
        "status": "done",
        "ts": time.time(),
    }


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "workers",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": BROKER})
    consumer.subscribe([JOBS_TOPIC])

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("worker up — consuming %s, group=workers", JOBS_TOPIC)
    while running:
        heartbeat()  # prove the poll loop is alive for the K8s liveness probe
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
            payload = json.loads(msg.value())
            log.info(
                "🔧 [worker] processing job %s from %s doing %s",
                payload["job_id"],
                payload["user"],
                payload["action"],
            )
            time.sleep(WORK_SECONDS)  # simulate heavy IO/compute
            event = build_event(payload)
            producer.produce(
                EVENTS_TOPIC,
                key=payload["user"].encode(),
                value=json.dumps(event).encode(),
            )
            producer.flush()
            consumer.commit(msg)  # at-least-once: commit after producing
        except Exception:
            log.exception("failed to handle message; skipping")

    consumer.close()
    log.info("worker shutting down")


if __name__ == "__main__":
    main()
