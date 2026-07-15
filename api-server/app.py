import json
import logging
import os
import time
import uuid

from celery import Celery
from confluent_kafka import Producer
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("api")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
JOBS_TOPIC = os.getenv("JOBS_TOPIC", "jobs")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

producer = Producer({"bootstrap.servers": BROKER})
# Enqueue Celery tasks by name only — api-server never imports task-server code.
celery = Celery("api", broker=REDIS_URL, backend=REDIS_URL)
app = FastAPI(title="api-server")


def _on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)
    else:
        log.info("queued to %s[%s]", msg.topic(), msg.partition())


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/stream")
async def stream_job(request: Request):
    """Kafka path: produce a job event onto the `jobs` topic (event stream)."""
    body = await request.json()
    user = body.get("user", "anonymous")
    action = body.get("action", "noop")
    job_id = str(uuid.uuid4())
    payload = {"job_id": job_id, "user": user, "action": action, "ts": time.time()}

    producer.produce(
        JOBS_TOPIC,
        key=user.encode(),
        value=json.dumps(payload).encode(),
        callback=_on_delivery,
    )
    producer.poll(0)
    log.info("📥 [api] streamed job %s from %s doing %s (Kafka)", job_id, user, action)
    return JSONResponse(status_code=202, content={"job_id": job_id})


@app.post("/api/queue")
async def queue_task(request: Request):
    """Celery path: enqueue an on-demand task onto Redis (task queue)."""
    body = await request.json()
    user = body.get("user", "anonymous")
    action = body.get("action", "noop")
    async_result = celery.send_task("process_task", args=[user, action])
    log.info("📥 [api] queued task %s for %s doing %s (Celery)", async_result.id, user, action)
    return JSONResponse(status_code=202, content={"task_id": async_result.id})
