# Celery + Celery Beat: Job Queue & Scheduling — Design

**Date:** 2026-07-14
**Status:** Approved for planning

## Goal

Extend the existing Kafka-based teaching backend to also teach **task queues** and
**job scheduling** via **Celery** and **Celery Beat**, while teaching Kafka's
**one-topic-many-subscribers** fan-out more forcefully. Everything runs from the
same single `docker compose up`, production-shaped.

Two async paradigms are taught **side by side under one codebase**:

- **Kafka track** (Redpanda) — an *event stream / log*. Emphasis: **one topic, many
  independent subscribers**, each getting the full stream.
- **Celery track** (Redis) — a *task queue* with a *scheduler*. Emphasis: **on-demand
  job queue** + **periodic job scheduling**.

## Non-goals / Explicitly unchanged

Per the user's constraint, these stay exactly as they are — they are good for learning:

- **nginx** as the single API gateway (path-based routing).
- **Docker / docker-compose** as the local orchestrator (still one command).
- **Redpanda / Kafka** and the existing `worker → events → notifier` flow.
- Kafka **topic names** (`jobs`, `events`) keep their *domain* names internally.

No shared broker: Kafka uses Redpanda, Celery uses Redis. They are independent at
the infrastructure level — itself a teaching point.

## Architecture

```
                                    ┌─ POST /api/stream ─► Redpanda(jobs) ─► worker-server ─► Redpanda(events) ─┐
curl :8080 ─► nginx ─► api-server ──┤                                                                           │
                                    └─ POST /api/queue ──► Redis ─► celery-worker                   (fan-out)    │
                                                            ▲                                                    ▼
                                                       celery-beat                      ┌───────────────┬───────────────┐
                                                   (every 30 seconds)          notification-server  analytic-server   ai-server
                                                                                (group:notifiers)   (group:analytics) (group:ai)
```

- `POST /api/stream` vs `POST /api/queue` is the centerpiece contrast: **same gateway,
  same api-server, two async backends** (Kafka event stream vs Celery task queue).
- The `events` topic now feeds **three** independent consumer groups. Each group
  receives every event — the fan-out lesson.

### HTTP path names the paradigm; topic keeps the domain name

`POST /api/stream` produces to the Kafka topic still named `jobs` (consumed by
`worker-server`). This is deliberate and called out in the README: the **HTTP
surface names the paradigm** (stream vs queue), while **topic names describe the
domain** (`jobs`, `events`). Renaming topics is out of scope (ripples through every
consumer + `.env`) and unnecessary for the lesson.

## Components

### New pulled-image infrastructure

| Service | Image | Role |
|---|---|---|
| `redis` | `redis:7-alpine` | Celery broker **and** result backend. Healthcheck: `redis-cli ping`. |
| `flower` | `mher/flower` | Celery monitoring web UI at **http://localhost:8082** — the Celery-side counterpart to Redpanda Console (:8081). Shows workers, live task flow (received/started/succeeded/failed), task args+results, and the Beat schedule with next-run countdowns. Points at `REDIS_URL`; `depends_on: redis`. No code of ours. |

**Two visual companions, one per paradigm:** Redpanda Console (`:8081`) for Kafka
topics/messages, Flower (`:8082`) for Celery workers/tasks/schedule.

### Renamed

`worker/` → `worker-server/` — for `*-server` naming consistency. Update:
- the directory name,
- `container_name` in `docker-compose.yml`,
- every reference in `docker-compose.yml` and `README.md`.

The source file **keeps the name `worker.py`** inside `worker-server/`, matching the
existing pattern where `notification-server/` holds `notifier.py` and
`monitor-server/` holds `monitor.py` (dir name ≠ file name is already the norm).

### New Kafka fan-out subscribers (on the `events` topic)

Each is self-contained (own `Dockerfile`, `requirements.txt`, source, pytest),
built exactly like the existing services, using `confluent-kafka`. Each has its own
**consumer group**, so each receives the full `events` stream independently.

| Service | Consumer group | Role | Log marker |
|---|---|---|---|
| `analytic-server` | `analytics` | Maintains an in-memory running tally of completed events (count per action, per user, total) and logs the current totals per event. | `📊 [analytics]` |
| `ai-server` | `ai` | Simulates AI enrichment of each completed job (e.g. "generated a summary/recommendation"). | `🤖 [ai]` |

`notification-server` (group `notifiers`) is unchanged; it becomes one of three
subscribers, making fan-out unmistakable.

### New Celery track (shared codebase, one directory)

`task-server/` holds the Celery app, task definitions, and Beat schedule
(`tasks.py`), plus its own `Dockerfile`, `requirements.txt` (adds `celery`,
`redis`), and pytest.

Two docker-compose services **share this one build** — the standard production
pattern (identical code, run either as a worker or as the scheduler). Celery's real
terminology is used intentionally (teaching value):

| Service | Command | Role |
|---|---|---|
| `celery-worker` | `celery -A tasks worker --loglevel=info` | Consumes tasks from Redis and runs them. |
| `celery-beat` | `celery -A tasks beat --loglevel=info` | Scheduler: enqueues periodic tasks on an interval. |

## Data flow & decoupling

### Services stay self-contained (no cross-imports)

`api-server` does **not** import `task-server`'s code. It holds its own tiny
`Celery(broker=REDIS_URL, backend=REDIS_URL)` instance and enqueues **by task name**:

```python
celery.send_task("process_task", args=[user, action])
```

Services share only the **broker + a task-name string** — exactly as they already
share Kafka *topic names*. This preserves the "independent deployables" principle.

### Celery tasks (defined in `task-server/tasks.py`)

| Task | Trigger | Behavior | Log marker |
|---|---|---|---|
| `process_task(user, action)` | On-demand via `POST /api/queue` (`send_task`) | Simulates work (`sleep(TASK_SECONDS)`), returns a result dict stored in the Redis backend. | `⚙️ [celery-worker]` |
| `heartbeat_report()` | Celery Beat, every `CELERY_BEAT_SECONDS` (default 30) | Logs a periodic report (demonstrates scheduling). | `⏰ [celery-beat]` |

The Beat schedule is configured on the Celery app
(`celery.conf.beat_schedule`), interval read from env.

### api-server changes

- Add `celery` + `redis` to `api-server/requirements.txt`; construct one `Celery`
  client pointed at `REDIS_URL`.
- **Rename** existing `POST /api/jobs` → **`POST /api/stream`** (Kafka producer,
  logic otherwise unchanged — still produces to the `jobs` topic).
- **Add** `POST /api/queue` → `celery.send_task("process_task", args=[user, action])`,
  returns `202` with the Celery `task_id`.
- `GET /api/health` unchanged.

## Configuration (`.env.example` additions)

```
# Celery / Redis
REDIS_URL=redis://redis:6379/0
CELERY_BEAT_SECONDS=30     # Beat fires heartbeat_report on this interval
TASK_SECONDS=1             # seconds of simulated work per Celery task
```

Existing Kafka vars (`KAFKA_BROKER`, `JOBS_TOPIC`, `EVENTS_TOPIC`, `WORK_SECONDS`)
are untouched.

## docker-compose changes

- **Add** `redis` (with `redis-cli ping` healthcheck).
- **Rename** `worker` service → `worker-server`.
- **Add** `analytic-server`, `ai-server` — `depends_on: redpanda (service_healthy)`.
- **Add** `celery-worker`, `celery-beat` — `depends_on: redis (service_healthy)`,
  both `build: ./task-server` with different `command:`.
- **Add** `flower` — `depends_on: redis (service_healthy)`, host port `8082:5555`.
- **api-server** now also `depends_on: redis (service_healthy)` (for enqueue),
  in addition to redpanda.

Container count goes **7 → 13**: `redpanda`, `redpanda-console`, `nginx`,
`api-server`, `monitor-server`, `worker-server`, `notification-server`,
`analytic-server`, `ai-server`, `redis`, `celery-worker`, `celery-beat`, `flower`.

## Testing

Match the existing style — each service has a pytest exercising a **pure function**:

- `analytic-server` — test the pure aggregation function (event → updated tallies).
- `ai-server` — test the pure enrichment/formatter function.
- `task-server` — test the pure task-body logic (task input → result dict), without
  a running broker.
- `api-server/test_app.py` — add a `POST /api/queue` test with `send_task` mocked
  (assert it's called with the right task name + args); rename existing job test to
  `/api/stream`.

Existing tests for other services remain green.

## Documentation (`README.md`)

- Update the **flow diagram** to include the Celery track + three-way fan-out.
- Update **container count** (7 → 13) and the run/watch commands
  (`docker compose logs -f ...` service list).
- Add a **"See the tasks"** subsection for Flower (http://localhost:8082),
  mirroring the existing **"See the messages"** Redpanda Console subsection —
  one visual companion per paradigm.
- Update the **"What each piece is (and its cloud equivalent)"** table with:
  - Redis → ElastiCache / Memorystore / Azure Cache for Redis
  - Celery worker → Celery on ECS/GKE, Cloud Tasks worker, etc.
  - Celery Beat → EventBridge Scheduler / Cloud Scheduler / Azure Scheduler
  - analytic-server, ai-server (event-driven consumers)
- Add a short **Kafka vs Celery** contrast section (stream + fan-out vs queue +
  scheduling; when to reach for each).
- Note the **HTTP-names-the-paradigm / topic-names-the-domain** distinction.
- Update the **project layout** tree (rename worker, add the new dirs).

## Success criteria

1. `docker compose up --build` starts all 13 containers; healthchecks pass.
2. `POST /api/stream` flows Kafka → `worker-server` → `events`, and **all three**
   subscribers (`notification-server`, `analytic-server`, `ai-server`) log the same
   event — visible fan-out.
3. `POST /api/queue` enqueues to Redis and `celery-worker` logs running the task.
4. `celery-beat` logs `heartbeat_report` every ~30s without any manual action.
   Flower (http://localhost:8082) shows the worker, the task history, and the
   Beat schedule.
5. `pytest` passes in every service directory.
6. nginx, Docker, Redpanda/Kafka, and the original job→event→notify flow are
   unchanged in behavior.
