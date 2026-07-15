import json
import logging
import os
import time
import uuid

from confluent_kafka import Producer
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("api")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
JOBS_TOPIC = os.getenv("JOBS_TOPIC", "jobs")

producer = Producer({"bootstrap.servers": BROKER})
app = FastAPI(title="api-server")


def _on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)
    else:
        log.info("queued to %s[%s]", msg.topic(), msg.partition())


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/jobs")
async def create_job(request: Request):
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
    log.info("📥 [api] accepted job %s from %s doing %s", job_id, user, action)
    return JSONResponse(status_code=202, content={"job_id": job_id})
