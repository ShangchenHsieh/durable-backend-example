# Celery + Celery Beat: Job Queue & Scheduling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Celery + Celery Beat task-queue/scheduling track (backed by Redis, monitored by Flower) alongside the existing Kafka flow, and strengthen the Kafka fan-out lesson with two new subscribers — all from one `docker compose up`.

**Architecture:** Two independent async paradigms under one compose file. Kafka (Redpanda) stays an event stream whose `events` topic now fans out to three consumer groups (`notifiers`, `analytics`, `ai`). Celery (Redis) is a task queue with a scheduler: `POST /api/queue` enqueues on demand, `celery-beat` fires a periodic task every 30s, `celery-worker` runs both, and Flower visualizes it. Services stay self-contained — api-server enqueues Celery tasks **by name** (`send_task`), never importing task-server code.

**Tech Stack:** Python 3.11, FastAPI, `confluent-kafka` (existing), `celery` + `redis` (new), Redpanda, Redis, Flower, Docker Compose, nginx (unchanged), pytest.

## Global Constraints

- **Do not change** nginx, Docker/compose-as-orchestrator, Redpanda/Kafka, or the existing job→event→notify behavior. `/api/*` already routes to api-server, so **no nginx edits**.
- Kafka **topic names stay** `jobs` and `events` (domain names). HTTP paths name the **paradigm**: `/api/stream` (Kafka), `/api/queue` (Celery).
- **Redis** is Celery's broker+backend only. **Nothing is shared** between Redpanda and Redis.
- Each service is **self-contained**: own `Dockerfile`, `requirements.txt`, source file, and a pytest exercising a **pure function** (match existing style — see `notification-server/`).
- Pinned versions: `celery==5.4.0`, `redis==5.0.7`, `redis:7-alpine`, `mher/flower:2.0`. Keep existing pins (`confluent-kafka==2.5.0`, `fastapi==0.111.0`, `uvicorn[standard]==0.30.1`, `httpx==0.27.0`, `pytest==8.2.2`).
- Env vars (added to `.env.example`): `REDIS_URL=redis://redis:6379/0`, `CELERY_BEAT_SECONDS=30`, `TASK_SECONDS=1`.
- Tests import modules directly (e.g. `import analytics`), so **run pytest from inside each service directory**.
- Final container count: **13**.

---

### Task 0: Initialize git (enables the frequent-commit workflow)

> Skip this task if you prefer not to use version control; if skipped, ignore every `git`/commit step below.

**Files:** none (repo-level).

- [ ] **Step 1: Initialize the repo**

```bash
cd /Users/sean/Projects/backend_example
git init
```

- [ ] **Step 2: Add a .gitignore for Python caches**

Create `.gitignore`:

```
__pycache__/
*.pyc
.pytest_cache/
.env
```

- [ ] **Step 3: Baseline commit of the current project**

```bash
git add -A
git commit -m "chore: baseline before adding Celery + fan-out"
```

Expected: a commit containing the existing project.

---

### Task 1: Rename `worker/` → `worker-server/`

**Files:**
- Rename dir: `worker/` → `worker-server/` (keeps `worker.py`, `test_worker.py`, `Dockerfile`, `requirements.txt` inside — file names unchanged, matching `notification-server/notifier.py`).
- Modify: `docker-compose.yml` (the `worker:` service block).

**Interfaces:**
- Produces: a service directory `worker-server/` built by compose service `worker-server`. No code/logic change.

- [ ] **Step 1: Move the directory (preserve history if using git)**

```bash
cd /Users/sean/Projects/backend_example
git mv worker worker-server   # or: mv worker worker-server  (if no git)
```

- [ ] **Step 2: Verify existing tests still pass under the new path**

```bash
cd /Users/sean/Projects/backend_example/worker-server && pytest -v
```

Expected: PASS (3 tests — heartbeat + build_event). No source changes needed.

- [ ] **Step 3: Update the compose service block**

In `docker-compose.yml`, replace the `worker` service block:

```yaml
  worker:
    build: ./worker
    container_name: worker
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

with:

```yaml
  worker-server:
    build: ./worker-server
    container_name: worker-server
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 4: Validate compose still parses**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
```

Expected: `OK` (no YAML/service errors).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename worker to worker-server for naming consistency"
```

---

### Task 2: Add Redis to compose + `.env.example`

**Files:**
- Modify: `docker-compose.yml` (add `redis` service).
- Modify: `.env.example` (add Celery/Redis vars).

**Interfaces:**
- Produces: a healthy `redis` service reachable at `redis://redis:6379/0`; env keys `REDIS_URL`, `CELERY_BEAT_SECONDS`, `TASK_SECONDS`.

- [ ] **Step 1: Append the Celery/Redis vars to `.env.example`**

Append to `.env.example`:

```
# Celery / Redis
REDIS_URL=redis://redis:6379/0
CELERY_BEAT_SECONDS=30
TASK_SECONDS=1
```

- [ ] **Step 2: Add the `redis` service to `docker-compose.yml`**

Add under the "Infrastructure" section (near `redpanda`):

```yaml
  redis:
    image: redis:7-alpine
    container_name: redis
    ports:
      - "6379:6379" # optional host access to Redis
    healthcheck:
      test: [ "CMD", "redis-cli", "ping" ]
      interval: 5s
      timeout: 3s
      retries: 12
```

- [ ] **Step 3: Validate and smoke-test Redis**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
docker compose up -d redis
docker compose exec redis redis-cli ping   # Expected: PONG
docker compose down
```

Expected: `OK` then `PONG`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: add Redis as the Celery broker/backend"
```

---

### Task 3: `task-server/` — Celery app, tasks, and Beat schedule

**Files:**
- Create: `task-server/tasks.py`
- Create: `task-server/test_tasks.py`
- Create: `task-server/requirements.txt`
- Create: `task-server/Dockerfile`

**Interfaces:**
- Produces:
  - Celery app object `celery` (name `"tasks"`), broker+backend = `REDIS_URL`.
  - Task registered as `"process_task"` with signature `process_task(user: str, action: str) -> dict`.
  - Task registered as `"heartbeat_report"`, scheduled by Beat every `CELERY_BEAT_SECONDS`.
  - Pure helpers: `build_result(user: str, action: str) -> dict`, `build_report(count: int) -> str`.
- Consumed by: Task 4 (runs this as worker+beat), Task 6 (api-server calls `send_task("process_task", args=[user, action])`).

- [ ] **Step 1: Write the failing tests**

Create `task-server/test_tasks.py`:

```python
import tasks


def test_build_result_marks_processed_and_keeps_fields():
    result = tasks.build_result("alice", "resize-image")
    assert result["user"] == "alice"
    assert result["action"] == "resize-image"
    assert result["status"] == "processed"


def test_build_report_contains_count_and_alive():
    line = tasks.build_report(7)
    assert "7" in line
    assert "alive" in line
```

- [ ] **Step 2: Write requirements so the test can import celery**

Create `task-server/requirements.txt`:

```
celery==5.4.0
redis==5.0.7
pytest==8.2.2
```

Then install locally for the test run:

```bash
cd /Users/sean/Projects/backend_example/task-server && pip install -r requirements.txt
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
cd /Users/sean/Projects/backend_example/task-server && pytest -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'tasks'`.

- [ ] **Step 4: Write `tasks.py`**

Create `task-server/tasks.py`:

```python
import logging
import os
import time

from celery import Celery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("celery")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
TASK_SECONDS = float(os.getenv("TASK_SECONDS", "1"))
BEAT_SECONDS = float(os.getenv("CELERY_BEAT_SECONDS", "30"))

# One Celery app; run as a worker or as beat (same code, different command).
celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.timezone = "UTC"
# Job scheduling: Beat enqueues heartbeat_report on a fixed interval.
celery.conf.beat_schedule = {
    "heartbeat-every-interval": {
        "task": "heartbeat_report",
        "schedule": BEAT_SECONDS,
    }
}

_beat_count = 0  # per-worker-process counter for the periodic report


def build_result(user: str, action: str) -> dict:
    """Pure: the result an on-demand task returns for a (user, action)."""
    return {"user": user, "action": action, "status": "processed"}


def build_report(count: int) -> str:
    """Pure: render the periodic heartbeat report line."""
    return f"⏰ [celery-beat] heartbeat #{count} — scheduler is alive"


@celery.task(name="process_task")
def process_task(user: str, action: str) -> dict:
    """On-demand task (job queue): target of POST /api/queue."""
    log.info("⚙️ [celery-worker] running task for %s doing %s", user, action)
    time.sleep(TASK_SECONDS)  # simulate work
    result = build_result(user, action)
    log.info("⚙️ [celery-worker] done: %s", result)
    return result


@celery.task(name="heartbeat_report")
def heartbeat_report() -> str:
    """Periodic task (job scheduling): fired by Celery Beat."""
    global _beat_count
    _beat_count += 1
    line = build_report(_beat_count)
    log.info(line)
    return line
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd /Users/sean/Projects/backend_example/task-server && pytest -v
```

Expected: PASS (2 tests).

- [ ] **Step 6: Write the Dockerfile**

Create `task-server/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tasks.py .
# Default to worker; docker-compose overrides `command` for celery-beat.
CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]
```

- [ ] **Step 7: Commit**

```bash
git add task-server/
git commit -m "feat: add task-server (Celery app, process_task, heartbeat_report)"
```

---

### Task 4: Add `celery-worker` + `celery-beat` to compose

**Files:**
- Modify: `docker-compose.yml` (two services sharing `build: ./task-server`).

**Interfaces:**
- Consumes: `task-server/` build (Task 3), `redis` (Task 2), `.env.example` (Task 2).
- Produces: running `celery-worker` (runs tasks) and `celery-beat` (scheduler).

- [ ] **Step 1: Add both services to `docker-compose.yml`**

Add under the "Our services" section:

```yaml
  celery-worker:
    build: ./task-server
    container_name: celery-worker
    command: [ "celery", "-A", "tasks", "worker", "--loglevel=info" ]
    env_file: .env.example
    depends_on:
      redis:
        condition: service_healthy

  celery-beat:
    build: ./task-server
    container_name: celery-beat
    command: [ "celery", "-A", "tasks", "beat", "--loglevel=info" ]
    env_file: .env.example
    depends_on:
      redis:
        condition: service_healthy
```

- [ ] **Step 2: Validate compose parses**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
```

Expected: `OK`.

- [ ] **Step 3: Verify Beat fires the periodic task end-to-end**

```bash
cd /Users/sean/Projects/backend_example
docker compose up -d --build redis celery-worker celery-beat
sleep 35
docker compose logs celery-worker | grep "heartbeat #"   # Expected: at least one line
docker compose down
```

Expected: at least one `⏰ [celery-beat] heartbeat #1 — scheduler is alive` line (Beat enqueued it, worker ran it).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: run task-server as celery-worker and celery-beat"
```

---

### Task 5: Add `flower` monitoring UI to compose

**Files:**
- Modify: `docker-compose.yml` (add `flower` service).

**Interfaces:**
- Consumes: `redis` (Task 2).
- Produces: Flower UI at `http://localhost:8082`.

- [ ] **Step 1: Add the `flower` service to `docker-compose.yml`**

Add under the "Infrastructure" section (near `redpanda-console`):

```yaml
  flower:
    image: mher/flower:2.0
    container_name: flower
    environment:
      CELERY_BROKER_URL: redis://redis:6379/0
    ports:
      - "8082:5555" # Celery UI: http://localhost:8082
    depends_on:
      redis:
        condition: service_healthy
```

- [ ] **Step 2: Validate and smoke-test Flower**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
docker compose up -d redis flower
sleep 5
curl -s -o /dev/null -w "%{http_code}\n" localhost:8082   # Expected: 200
docker compose down
```

Expected: `OK` then `200`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Flower UI for Celery monitoring on :8082"
```

---

### Task 6: api-server — rename `/api/jobs` → `/api/stream`, add `/api/queue`

**Files:**
- Modify: `api-server/app.py`
- Modify: `api-server/requirements.txt`
- Modify: `api-server/test_app.py`
- Modify: `docker-compose.yml` (api-server also `depends_on: redis`)

**Interfaces:**
- Consumes: Celery task name `"process_task"` (Task 3) via `celery.send_task`; `REDIS_URL` (Task 2).
- Produces: `POST /api/stream` (Kafka producer → `jobs` topic, returns `{"job_id": ...}`), `POST /api/queue` (Celery enqueue, returns `{"task_id": ...}`). `GET /api/health` unchanged.

- [ ] **Step 1: Update the tests (rename job test, add queue test)**

Replace the body of `api-server/test_app.py` with:

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


class FakeAsyncResult:
    id = "task-123"


def test_health_ok():
    client = TestClient(api.app)
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_stream_returns_202_and_produces(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(api, "producer", fake)

    client = TestClient(api.app)
    resp = client.post("/api/stream", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert "job_id" in resp.json()
    assert fake.captured["topic"] == "jobs"   # topic keeps its domain name
    assert fake.captured["key"] == b"alice"
    assert b"resize-image" in fake.captured["value"]


def test_queue_returns_202_and_sends_celery_task(monkeypatch):
    captured = {}

    def fake_send_task(name, args=None):
        captured["name"] = name
        captured["args"] = args
        return FakeAsyncResult()

    monkeypatch.setattr(api.celery, "send_task", fake_send_task)

    client = TestClient(api.app)
    resp = client.post("/api/queue", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert resp.json()["task_id"] == "task-123"
    assert captured["name"] == "process_task"
    assert captured["args"] == ["alice", "resize-image"]
```

- [ ] **Step 2: Add celery+redis to api-server requirements and install**

Replace `api-server/requirements.txt` with:

```
fastapi==0.111.0
uvicorn[standard]==0.30.1
confluent-kafka==2.5.0
celery==5.4.0
redis==5.0.7
httpx==0.27.0
pytest==8.2.2
```

Then install locally:

```bash
cd /Users/sean/Projects/backend_example/api-server && pip install -r requirements.txt
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/sean/Projects/backend_example/api-server && pytest -v
```

Expected: FAIL — `/api/stream` returns 404 and `api.celery` does not exist yet.

- [ ] **Step 4: Update `app.py`**

Replace `api-server/app.py` with:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/sean/Projects/backend_example/api-server && pytest -v
```

Expected: PASS (3 tests: health, stream, queue).

- [ ] **Step 6: Add redis to api-server's compose deps**

In `docker-compose.yml`, replace the api-server `depends_on` block:

```yaml
    depends_on:
      redpanda:
        condition: service_healthy
```

with:

```yaml
    depends_on:
      redpanda:
        condition: service_healthy
      redis:
        condition: service_healthy
```

(Apply only to the `api-server` service block.)

- [ ] **Step 7: Validate compose parses**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
```

Expected: `OK`.

- [ ] **Step 8: Commit**

```bash
git add api-server/ docker-compose.yml
git commit -m "feat: api-server /api/stream (Kafka) + /api/queue (Celery)"
```

---

### Task 7: `analytic-server/` — new Kafka subscriber (group `analytics`)

**Files:**
- Create: `analytic-server/analytics.py`
- Create: `analytic-server/test_analytics.py`
- Create: `analytic-server/requirements.txt`
- Create: `analytic-server/Dockerfile`
- Modify: `docker-compose.yml` (add `analytic-server`)

**Interfaces:**
- Consumes: `events` topic (Kafka), consumer group `analytics`.
- Produces: pure helpers `update_tally(tally: Counter, event: dict) -> Counter`, `format_stats(tally: Counter) -> str`.

- [ ] **Step 1: Write the failing tests**

Create `analytic-server/test_analytics.py`:

```python
from collections import Counter

import analytics


def test_update_tally_counts_actions():
    tally = Counter()
    analytics.update_tally(tally, {"action": "resize-image"})
    analytics.update_tally(tally, {"action": "resize-image"})
    analytics.update_tally(tally, {"action": "transcode"})
    assert tally["resize-image"] == 2
    assert tally["transcode"] == 1


def test_format_stats_shows_total_and_breakdown():
    tally = Counter({"resize-image": 2, "transcode": 1})
    line = analytics.format_stats(tally)
    assert "3" in line
    assert "resize-image=2" in line
    assert "transcode=1" in line
```

- [ ] **Step 2: Write requirements and install**

Create `analytic-server/requirements.txt`:

```
confluent-kafka==2.5.0
pytest==8.2.2
```

```bash
cd /Users/sean/Projects/backend_example/analytic-server && pip install -r requirements.txt
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/sean/Projects/backend_example/analytic-server && pytest -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'analytics'`.

- [ ] **Step 4: Write `analytics.py`**

Create `analytic-server/analytics.py`:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/sean/Projects/backend_example/analytic-server && pytest -v
```

Expected: PASS (2 tests).

- [ ] **Step 6: Write the Dockerfile**

Create `analytic-server/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY analytics.py .
CMD ["python", "-u", "analytics.py"]
```

- [ ] **Step 7: Add the service to `docker-compose.yml`**

```yaml
  analytic-server:
    build: ./analytic-server
    container_name: analytic-server
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 8: Validate compose and commit**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
git add analytic-server/ docker-compose.yml
git commit -m "feat: add analytic-server (events subscriber, group=analytics)"
```

Expected: `OK`.

---

### Task 8: `ai-server/` — new Kafka subscriber (group `ai`)

**Files:**
- Create: `ai-server/ai.py`
- Create: `ai-server/test_ai.py`
- Create: `ai-server/requirements.txt`
- Create: `ai-server/Dockerfile`
- Modify: `docker-compose.yml` (add `ai-server`)

**Interfaces:**
- Consumes: `events` topic (Kafka), consumer group `ai`.
- Produces: pure helper `enrich(event: dict) -> str`.

- [ ] **Step 1: Write the failing test**

Create `ai-server/test_ai.py`:

```python
import ai


def test_enrich_mentions_job_user_and_action():
    event = {
        "job_id": "j1",
        "user": "alice",
        "action": "resize-image",
        "status": "done",
        "ts": 1.0,
    }
    line = ai.enrich(event)
    assert "j1" in line
    assert "alice" in line
    assert "resize-image" in line
```

- [ ] **Step 2: Write requirements and install**

Create `ai-server/requirements.txt`:

```
confluent-kafka==2.5.0
pytest==8.2.2
```

```bash
cd /Users/sean/Projects/backend_example/ai-server && pip install -r requirements.txt
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd /Users/sean/Projects/backend_example/ai-server && pytest -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'ai'`.

- [ ] **Step 4: Write `ai.py`**

Create `ai-server/ai.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd /Users/sean/Projects/backend_example/ai-server && pytest -v
```

Expected: PASS (1 test).

- [ ] **Step 6: Write the Dockerfile**

Create `ai-server/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ai.py .
CMD ["python", "-u", "ai.py"]
```

- [ ] **Step 7: Add the service to `docker-compose.yml`**

```yaml
  ai-server:
    build: ./ai-server
    container_name: ai-server
    env_file: .env.example
    depends_on:
      redpanda:
        condition: service_healthy
```

- [ ] **Step 8: Validate compose and commit**

```bash
cd /Users/sean/Projects/backend_example && docker compose config >/dev/null && echo OK
git add ai-server/ docker-compose.yml
git commit -m "feat: add ai-server (events subscriber, group=ai)"
```

Expected: `OK`.

---

### Task 9: Update `README.md`

**Files:**
- Modify: `README.md`

**Interfaces:** documentation only — must reflect 13 containers, both endpoints, fan-out, Celery track, Flower.

- [ ] **Step 1: Replace the intro + flow diagram**

Replace lines 3–20 (the intro paragraph and the `## The flow` code block) with:

```markdown
A minimal, readable example of a modern scalable backend that teaches **two async
paradigms side by side**: a Kafka **event stream** (one topic, many subscribers) and
a Celery **task queue + scheduler** — fronted by an **API gateway**, all runnable
locally with one command.

## The flow

```
                                     ┌─ POST /api/stream ─► Redpanda(jobs) ─► worker-server ─► Redpanda(events) ─┐
curl :8080 ─► nginx ─► api-server ───┤                                                                          │
                                     └─ POST /api/queue ──► Redis ─► celery-worker                  (fan-out)    │
                                                            ▲                                                    ▼
                                                       celery-beat                    ┌──────────────┬──────────────┐
                                                  (every 30 seconds)          notification-server  analytic-server  ai-server
                                                                              (group:notifiers)   (group:analytics) (group:ai)
```

- **Kafka track** (`/api/stream`): one `events` topic fans out to **three** independent
  consumer groups — each receives every event.
- **Celery track** (`/api/queue`): on-demand tasks queue through Redis; **Celery Beat**
  also fires a periodic task every 30s. Same gateway, two async backends.
```

- [ ] **Step 2: Update the "Run it" section (container count + example calls)**

Replace the `## Run it` body (old lines 24–45) with:

```markdown
```bash
docker compose up --build
```

This starts **thirteen** containers: `nginx`, `api-server`, `monitor-server`,
`worker-server`, `notification-server`, `analytic-server`, `ai-server`, `redis`,
`celery-worker`, `celery-beat`, `flower`, `redpanda` (the broker), and
`redpanda-console` (a UI).

Then, in another terminal, try **both** paradigms:

```bash
# Kafka event stream → worker-server → events → 3 subscribers
curl -X POST localhost:8080/api/stream \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'

# Celery task queue → celery-worker
curl -X POST localhost:8080/api/queue \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
```

Watch the flow:

```bash
docker compose logs -f api-server worker-server notification-server \
  analytic-server ai-server celery-worker celery-beat
```

For `/api/stream` you'll see the job accepted (`📥`), processed (`🔧`), and then
**all three** subscribers react: notify (`🔔`), analytics (`📊`), ai (`🤖`). For
`/api/queue` you'll see the Celery worker run it (`⚙️`). Celery Beat logs `⏰` every
30s on its own.
```

- [ ] **Step 3: Update the "See the messages" section — add "See the tasks"**

Replace the `## See the messages` section (old lines 64–66) with:

```markdown
## See the messages (Kafka)

Open **Redpanda Console** at http://localhost:8081 to watch messages land in the
`jobs` and `events` topics, and to see the three consumer groups on `events`.

## See the tasks (Celery)

Open **Flower** at http://localhost:8082 to watch Celery workers, live task flow
(received/started/succeeded/failed), task args + results, and the **Beat schedule**
with a countdown to the next `heartbeat_report`.
```

- [ ] **Step 4: Update the "What each piece is" table**

In the `## What each piece is (and its cloud equivalent)` table, rename the `worker`
row to `worker-server`, and add these rows:

```markdown
| analytic-server     | Event-driven analytics (fan-out)      | ECS/Lambda          | Cloud Run       | Container Apps |
| ai-server           | Event-driven AI enrichment (fan-out)  | SageMaker + Lambda  | Vertex + Run    | Functions |
| Redis               | Celery broker + result backend        | ElastiCache         | Memorystore     | Azure Cache for Redis |
| celery-worker       | Task-queue worker (on-demand jobs)    | ECS worker          | Cloud Run Jobs  | Container Apps job |
| celery-beat         | Periodic job scheduler                | EventBridge Scheduler | Cloud Scheduler | Logic Apps / Timer |
| Flower              | Celery monitoring UI                  | —                   | —               | — |
```

- [ ] **Step 5: Add a "Kafka vs Celery" contrast section**

Insert after the "What each piece is" table, before "## Can I emulate...":

```markdown
## Kafka vs Celery — two async tools

| | Kafka (Redpanda) | Celery (Redis) |
|---|---|---|
| Shape | Event **stream / log** | **Task queue** + scheduler |
| Fan-out | One topic → **many** consumer groups, each gets every message | One task → **one** worker runs it |
| Scheduling | none built-in | **Celery Beat** fires periodic tasks |
| In this repo | `/api/stream` → 3 subscribers | `/api/queue` + `celery-beat` |
| Reach for it when | multiple services react to the same events | you need background jobs or cron-like schedules |

Note: the HTTP path names the **paradigm** (`/api/stream`, `/api/queue`); the Kafka
topic keeps its **domain** name (`jobs`). That mismatch is intentional.
```

- [ ] **Step 6: Update the "Project layout" tree**

Replace the project-layout code block (old lines 119–128) with:

```markdown
```
backend_example/
├── docker-compose.yml          # wires all thirteen containers together
├── .env.example                # broker addresses, topics, Celery/Redis settings
├── nginx/nginx.conf            # the API gateway config (routes /api and /monitor)
├── api-server/                 # FastAPI: /api/stream → Kafka, /api/queue → Celery
├── monitor-server/             # FastAPI: GET /monitor → a second routing target
├── worker-server/              # consumes `jobs` → simulates work → produces `events`
├── notification-server/        # consumes `events` (group: notifiers)
├── analytic-server/            # consumes `events` (group: analytics) — fan-out
├── ai-server/                  # consumes `events` (group: ai) — fan-out
└── task-server/                # Celery app: process_task + heartbeat_report (Beat)
```
```

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs: document Celery track, fan-out, Flower, and /api/stream|/api/queue"
```

---

### Task 10: Full-stack integration verification

**Files:** none (verification only).

- [ ] **Step 1: Build and start the whole stack**

```bash
cd /Users/sean/Projects/backend_example
docker compose up -d --build
```

- [ ] **Step 2: Confirm all 13 containers are up**

```bash
docker compose ps --format '{{.Name}} {{.State}}'
```

Expected: 13 rows, all `running` (redpanda/redis show healthy).

- [ ] **Step 3: Exercise the Kafka fan-out**

```bash
curl -s -X POST localhost:8080/api/stream \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
sleep 5
docker compose logs notification-server analytic-server ai-server | \
  grep -E "🔔|📊|🤖"
```

Expected: at least one line each from `🔔` (notifiers), `📊` (analytics), `🤖` (ai)
for the **same** event — fan-out confirmed.

- [ ] **Step 4: Exercise the Celery queue**

```bash
curl -s -X POST localhost:8080/api/queue \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
sleep 3
docker compose logs celery-worker | grep "⚙️"
```

Expected: `⚙️ [celery-worker] running task ...` then `done`.

- [ ] **Step 5: Confirm Beat scheduling and the UIs**

```bash
docker compose logs celery-worker | grep "heartbeat #"         # Beat fired
curl -s -o /dev/null -w "console:%{http_code}\n" localhost:8081 # Expected: 200
curl -s -o /dev/null -w "flower:%{http_code}\n"  localhost:8082 # Expected: 200
```

Expected: at least one `heartbeat #` line, `console:200`, `flower:200`.

- [ ] **Step 6: Tear down**

```bash
docker compose down
```

- [ ] **Step 7: Final commit**

```bash
git add -A
git commit -m "chore: Celery + Celery Beat + Kafka fan-out complete" --allow-empty
```

---

## Notes for the implementer

- **pip installs** in test steps assume a local Python 3.11 venv. If you only intend to run tests inside Docker, you may instead run each service's tests with `docker compose run --rm --entrypoint pytest <service> -v` — but the local path is faster for TDD.
- **`docker compose config`** is the cheap gate after every compose edit; run it before building.
- If a compose `up` step leaves containers running from a prior task, `docker compose down` first to free ports (6379, 8081, 8082, 8080).
