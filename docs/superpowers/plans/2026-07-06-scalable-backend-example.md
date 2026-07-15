# Scalable Backend Example Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable, heavily-readable example backend (nginx + Redpanda + api-server + worker + notification-server) that teaches modern scalable backend architecture, up with one `docker compose up --build`.

**Architecture:** Three self-contained Python services decoupled by a Kafka-compatible broker (Redpanda). nginx fronts the API as a gateway. The API produces `jobs`; the worker consumes `jobs`, does simulated work, and produces `events`; the notification-server consumes `events` and logs a notification. Redpanda Console gives a web UI to watch messages.

**Tech Stack:** Python 3.11-slim, FastAPI + uvicorn (api), `confluent-kafka` client, Redpanda broker, Redpanda Console, nginx, docker-compose.

## Global Constraints

- Python base image: `python:3.11-slim` for all three built services.
- Kafka client library: `confluent-kafka` (not `kafka-python`).
- Broker internal address: `redpanda:9092`. Config passed via env vars.
- Topics: `jobs` (api→worker), `events` (worker→notifier). Auto-created on first produce.
- Consumer groups: `workers` (worker), `notifiers` (notification-server).
- Published host ports: nginx `8080:80`, redpanda-console `8081:8080`, redpanda `19092:19092`.
- Each service is fully self-contained (own Dockerfile + requirements.txt + source + tests). Duplication of small Kafka boilerplate is intentional.
- Repo is not yet under git. Commit steps assume you ran `git init` once first; commits are optional for this learning project — skip if you prefer.
- Log lines use the exact emoji-prefixed formats specified so the end-to-end flow is easy to spot in `docker compose logs -f`.

---

### Task 1: Broker foundation (Redpanda + Console + env)

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`

**Interfaces:**
- Produces: a `redpanda` service reachable at `redpanda:9092` inside the compose network and `localhost:19092` from the host; a `redpanda-console` web UI at `localhost:8081`. Later tasks add their service blocks to this same `docker-compose.yml`.

- [ ] **Step 1: Create `.env.example`**

```dotenv
# Kafka / Redpanda
KAFKA_BROKER=redpanda:9092
JOBS_TOPIC=jobs
EVENTS_TOPIC=events

# Worker: seconds of simulated work per job
WORK_SECONDS=1
```

- [ ] **Step 2: Create `docker-compose.yml` with the broker + console**

```yaml
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    container_name: redpanda
    command:
      - redpanda
      - start
      - --mode=dev-container
      - --smp=1
      - --default-log-level=info
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda:9092,external://localhost:19092
    ports:
      - "19092:19092"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true'"]
      interval: 5s
      timeout: 3s
      retries: 12

  redpanda-console:
    image: docker.redpanda.com/redpandadata/console:v2.7.2
    container_name: redpanda-console
    environment:
      KAFKA_BROKERS: redpanda:9092
    ports:
      - "8081:8080"
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 3: Bring up the broker and verify health**

Run: `docker compose up -d redpanda redpanda-console`
Then: `docker compose ps`
Expected: `redpanda` shows `healthy`; `redpanda-console` is `running`.

- [ ] **Step 4: Verify the Console UI is reachable**

Run: `curl -s -o /dev/null -w "%{http_code}" localhost:8081`
Expected: `200`

- [ ] **Step 5: Tear down**

Run: `docker compose down`

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: redpanda broker + console foundation"
```

---

### Task 2: api-server (FastAPI producer)

**Files:**
- Create: `api-server/app.py`
- Create: `api-server/test_app.py`
- Create: `api-server/requirements.txt`
- Create: `api-server/Dockerfile`
- Modify: `docker-compose.yml` (add `api-server` service)

**Interfaces:**
- Consumes: env `KAFKA_BROKER`, `JOBS_TOPIC`.
- Produces: HTTP `GET /health` → `200 {"status":"ok"}`; `POST /jobs` with body `{"user","action"}` → `202 {"job_id": <uuid4 str>}`, and a message on the `jobs` topic with value `{"job_id","user","action","ts"}` keyed by `user`. Module-level `app` (FastAPI) and `producer` (confluent_kafka.Producer).

- [ ] **Step 1: Create `api-server/requirements.txt`**

```text
fastapi==0.111.0
uvicorn[standard]==0.30.1
confluent-kafka==2.5.0
httpx==0.27.0
pytest==8.2.2
```

- [ ] **Step 2: Write the failing tests `api-server/test_app.py`**

```python
import app as api
from fastapi.testclient import TestClient


class FakeProducer:
    """Stand-in for confluent_kafka.Producer (a C object we can't patch methods on)."""

    def __init__(self):
        self.captured = {}

    def produce(self, topic, key=None, value=None, callback=None):
        self.captured = {"topic": topic, "key": key, "value": value}

    def poll(self, _timeout):
        return 0


def test_health_ok():
    client = TestClient(api.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_create_job_returns_202_and_produces(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(api, "producer", fake)

    client = TestClient(api.app)
    resp = client.post("/jobs", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert "job_id" in resp.json()
    assert fake.captured["topic"] == "jobs"
    assert fake.captured["key"] == b"alice"
    assert b"resize-image" in fake.captured["value"]
```

Note: `confluent_kafka.Producer` is a C extension type — you cannot `setattr`
a method onto an instance. Replace the whole module-level `producer` object
instead (the route resolves `producer` as a module global at call time).

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd api-server && pip install -r requirements.txt && python -m pytest -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app'`.

- [ ] **Step 4: Write minimal implementation `api-server/app.py`**

```python
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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs")
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd api-server && python -m pytest -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Create `api-server/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 7: Add the `api-server` service to `docker-compose.yml`**

```yaml
  api-server:
    build: ./api-server
    container_name: api-server
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 8: Verify the image builds**

Run: `docker compose build api-server`
Expected: build completes with no error.

- [ ] **Step 9: Commit**

```bash
git add api-server docker-compose.yml
git commit -m "feat: api-server produces jobs to kafka"
```

---

### Task 3: worker (consume jobs → produce events)

**Files:**
- Create: `worker/worker.py`
- Create: `worker/test_worker.py`
- Create: `worker/requirements.txt`
- Create: `worker/Dockerfile`
- Modify: `docker-compose.yml` (add `worker` service)

**Interfaces:**
- Consumes: `jobs` topic messages `{"job_id","user","action","ts"}`; env `KAFKA_BROKER`, `JOBS_TOPIC`, `EVENTS_TOPIC`, `WORK_SECONDS`.
- Produces: `events` topic messages `{"job_id","user","action","status":"done","ts"}` keyed by `user`. Pure function `build_event(payload: dict) -> dict`.

- [ ] **Step 1: Create `worker/requirements.txt`**

```text
confluent-kafka==2.5.0
pytest==8.2.2
```

- [ ] **Step 2: Write the failing test `worker/test_worker.py`**

```python
import worker


def test_build_event_marks_done_and_keeps_fields():
    payload = {"job_id": "j1", "user": "alice", "action": "resize-image", "ts": 1.0}
    event = worker.build_event(payload)
    assert event["job_id"] == "j1"
    assert event["user"] == "alice"
    assert event["action"] == "resize-image"
    assert event["status"] == "done"
    assert "ts" in event
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd worker && pip install -r requirements.txt && python -m pytest -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'worker'`.

- [ ] **Step 4: Write minimal implementation `worker/worker.py`**

```python
import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("worker")

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
JOBS_TOPIC = os.getenv("JOBS_TOPIC", "jobs")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")
WORK_SECONDS = float(os.getenv("WORK_SECONDS", "1"))


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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd worker && python -m pytest -v`
Expected: PASS (1 passed).

- [ ] **Step 6: Create `worker/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker.py .
CMD ["python", "-u", "worker.py"]
```

- [ ] **Step 7: Add the `worker` service to `docker-compose.yml`**

```yaml
  worker:
    build: ./worker
    container_name: worker
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 8: Verify the image builds**

Run: `docker compose build worker`
Expected: build completes with no error.

- [ ] **Step 9: Commit**

```bash
git add worker docker-compose.yml
git commit -m "feat: worker consumes jobs and emits done events"
```

---

### Task 4: notification-server (consume events → notify)

**Files:**
- Create: `notification-server/notifier.py`
- Create: `notification-server/test_notifier.py`
- Create: `notification-server/requirements.txt`
- Create: `notification-server/Dockerfile`
- Modify: `docker-compose.yml` (add `notification-server` service)

**Interfaces:**
- Consumes: `events` topic messages `{"job_id","user","action","status","ts"}`; env `KAFKA_BROKER`, `EVENTS_TOPIC`.
- Produces: log line only. Pure function `format_notification(event: dict) -> str`.

- [ ] **Step 1: Create `notification-server/requirements.txt`**

```text
confluent-kafka==2.5.0
pytest==8.2.2
```

- [ ] **Step 2: Write the failing test `notification-server/test_notifier.py`**

```python
import notifier


def test_format_notification_contains_user_and_action():
    event = {
        "job_id": "j1",
        "user": "alice",
        "action": "resize-image",
        "status": "done",
        "ts": 1.0,
    }
    line = notifier.format_notification(event)
    assert "alice" in line
    assert "resize-image" in line
    assert "j1" in line
    assert "done" in line
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd notification-server && pip install -r requirements.txt && python -m pytest -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notifier'`.

- [ ] **Step 4: Write minimal implementation `notification-server/notifier.py`**

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd notification-server && python -m pytest -v`
Expected: PASS (1 passed).

- [ ] **Step 6: Create `notification-server/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY notifier.py .
CMD ["python", "-u", "notifier.py"]
```

- [ ] **Step 7: Add the `notification-server` service to `docker-compose.yml`**

```yaml
  notification-server:
    build: ./notification-server
    container_name: notification-server
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 8: Verify the image builds**

Run: `docker compose build notification-server`
Expected: build completes with no error.

- [ ] **Step 9: Commit**

```bash
git add notification-server docker-compose.yml
git commit -m "feat: notification-server logs notifications from events"
```

---

### Task 5: nginx gateway + full-stack wiring

**Files:**
- Create: `nginx/nginx.conf`
- Modify: `docker-compose.yml` (add `nginx` service)

**Interfaces:**
- Consumes: `api-server:8000` upstream.
- Produces: host `localhost:8080` → proxied to the API. This is the only entrypoint clients use.

- [ ] **Step 1: Create `nginx/nginx.conf`**

```nginx
events {}

http {
    upstream api {
        server api-server:8000;
        # To load-balance across replicas, scale api-server and add entries:
        #   docker compose up --scale api-server=3
        # nginx will round-robin across the resolved instances.
    }

    server {
        listen 80;

        location / {
            proxy_pass http://api;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
        }
    }
}
```

- [ ] **Step 2: Add the `nginx` service to `docker-compose.yml`**

```yaml
  nginx:
    image: nginx:1.27-alpine
    container_name: nginx
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    ports:
      - "8080:80"
    depends_on:
      - api-server
```

- [ ] **Step 3: Bring up the whole stack**

Run: `docker compose up --build -d`
Then: `docker compose ps`
Expected: all six containers up; `redpanda` healthy.

- [ ] **Step 4: Verify a request flows through nginx → api-server**

Run: `curl -s -o /dev/null -w "%{http_code}" localhost:8080/health`
Expected: `200`

- [ ] **Step 5: Verify POST /jobs through the gateway returns 202**

Run: `curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' -d '{"user":"alice","action":"resize-image"}'`
Expected: JSON containing a `job_id`.

- [ ] **Step 6: Leave the stack running for Task 6, then commit**

```bash
git add nginx docker-compose.yml
git commit -m "feat: nginx api gateway fronting the api-server"
```

---

### Task 6: README + end-to-end verification

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: the running stack from Task 5.
- Produces: documentation (run instructions, flow diagram, cloud-mapping table).

- [ ] **Step 1: End-to-end smoke — post a job and watch the flow**

Run (stack already up from Task 5):
```bash
curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
docker compose logs --since 30s worker notification-server
```
Expected: worker log contains `🔧 [worker] processing job ... from alice doing resize-image`; notification-server log contains `🔔 [notify] Hey alice — your job ... (resize-image) is done!`.

- [ ] **Step 2: Verify messages are visible in Redpanda Console**

Open `http://localhost:8081` → Topics → confirm `jobs` and `events` exist and each has at least one message.

- [ ] **Step 3: Create `README.md`**

````markdown
# Scalable Backend Example

A minimal, readable example of a modern scalable backend: an **API gateway**, a
stateless **API tier**, an async **worker tier**, and an event-driven
**notification tier**, decoupled by a **Kafka-compatible broker** — all runnable
locally with one command.

## The flow

```
curl :8080  ─►  nginx  ─►  api-server ──(topic: jobs)──►  Redpanda
  (host)     (gateway)                                       │
                                                consume jobs │
                                                             ▼
                                                          worker ──(topic: events)──► Redpanda
                                                                                         │
                                                                            consume events│
                                                                                         ▼
                                                                              notification-server
```

## Run it

```bash
docker compose up --build
```

Then, in another terminal:

```bash
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
```

Watch the flow:

```bash
docker compose logs -f api-server worker notification-server
```

You'll see the job accepted (`📥`), processed (`🔧`), and notified (`🔔`).

## See the messages

Open **Redpanda Console** at http://localhost:8081 to watch messages land in the
`jobs` and `events` topics.

## What each piece is (and its cloud equivalent)

| This project        | What it does                          | AWS               | GCP             | Azure |
|---------------------|---------------------------------------|-------------------|-----------------|-------|
| nginx               | API gateway / load balancer           | API Gateway / ALB | Cloud LB        | App Gateway |
| api-server          | Stateless request handling            | ECS/Fargate, Lambda | Cloud Run     | Container Apps |
| Redpanda            | Durable message queue (Kafka API)     | MSK / SQS         | Pub/Sub         | Event Hubs |
| worker              | Async heavy IO/compute processing     | ECS worker, Lambda | Cloud Run Jobs | Container Apps job |
| notification-server | Event-driven fan-out to users         | SNS + Lambda      | Pub/Sub push    | Functions |
| docker-compose      | Local orchestration                   | ECS / EKS         | GKE             | AKS |

## Can I emulate the whole cloud with Docker?

Largely, yes — this project already emulates the gateway + queue + compute tiers.
A fuller local "cloud" would add:

- **MinIO** — S3-compatible object storage (for the images a real worker resizes).
- **Postgres** — a relational database (stand-in for RDS/Cloud SQL).
- **Multiple api-server replicas** — `docker compose up --scale api-server=3` to
  see nginx round-robin real load balancing.
- **Prometheus + Grafana + Jaeger** — metrics, dashboards, and distributed tracing.
- **A secrets store** — e.g. HashiCorp Vault, for credentials.

## Why these choices

- **Redpanda** is Kafka-wire-compatible but a single binary with no Zookeeper —
  the Python code uses a normal Kafka client and can't tell the difference.
- **`confluent-kafka`** is the production-standard client.
- Each service is **self-contained** (its own Dockerfile + deps) to reinforce
  that microservices are independent deployables that talk only over Kafka/HTTP.
- The worker uses **at-least-once** delivery (commits offsets only after emitting
  the downstream event), a deliberate, common real-world tradeoff.
````

- [ ] **Step 4: Tear the stack down cleanly**

Run: `docker compose down`
Expected: all containers removed without error.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: README with run instructions and cloud mapping"
```

---

## Self-Review Notes

- **Spec coverage:** broker (Task 1), api-server (Task 2), worker (Task 3), notifier (Task 4), nginx gateway (Task 5), README + cloud mapping + e2e verification (Task 6). All spec sections covered.
- **Ports:** nginx 8080, console 8081, redpanda 19092 — no collisions (Redpanda's external schema-registry would be 18081, not published here).
- **Type consistency:** message shapes (`job_id/user/action/ts`, plus `status` on events) consistent across api → worker → notifier; `build_event` and `format_notification` signatures match their tests.
