# Scalable Backend Example — Design

**Date:** 2026-07-06
**Author:** Shang-Chen Hsieh (with Claude)
**Status:** Approved for implementation planning

## Goal

Build a small, heavily-readable example backend that teaches **modern scalable
backend architecture**: an API tier, an async worker tier, and an event-driven
notification tier, decoupled by a Kafka-compatible message broker, fronted by an
nginx gateway — all runnable locally with a single `docker compose up`.

This is a **learning artifact**, not a production system. Optimize for clarity
and observability over features. Code should be simple enough to read top to
bottom and understand the flow.

## Non-Goals

- No database / persistent storage (state lives in Kafka + memory only).
- No authentication, no real image processing, no external side effects.
- No production concerns (TLS, secrets management, autoscaling policies). These
  are *mentioned* in the README's cloud-mapping section but not implemented.

## Technology Choices

| Concern              | Choice                        | Why |
|----------------------|-------------------------------|-----|
| Language             | Python 3.11 (slim)            | Most readable for learning the architecture. |
| API framework        | FastAPI + uvicorn             | Minimal, modern, self-documenting. |
| Kafka client         | `confluent-kafka`             | Production-standard (librdkafka); works transparently against Redpanda. |
| Broker               | Redpanda (`redpandadata/redpanda`) | Kafka-wire-compatible, single binary, no Zookeeper, ~2s startup. Python code can't tell it apart from Apache Kafka. |
| Gateway / proxy      | nginx (official image)        | Plays the "API Gateway / load balancer" role. |
| Broker UI (learning) | Redpanda Console (`redpandadata/console`) | Web UI to *see* messages land in topics. |
| Orchestration        | docker-compose (single file)  | Local stand-in for K8s/ECS. |

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              docker-compose network          │
  curl POST :8080   │   ┌────────┐      ┌──────────────┐           │
  ────────────────► │   │ nginx  │────► │  api-server  │           │
   (host)           │   │ :80    │      │  FastAPI     │           │
                    │   └────────┘      └──────┬───────┘           │
                    │  (API Gateway)           │ produce           │
                    │                          ▼  topic: jobs      │
                    │                   ┌──────────────┐           │
                    │                   │   Redpanda   │◄────┐     │
                    │                   │ (Kafka API)  │     │     │
                    │                   └──────┬───────┘     │     │
                    │                consume   │ jobs        │     │
                    │                          ▼             │     │
                    │                   ┌──────────────┐     │     │
                    │                   │    worker    │─────┘     │
                    │                   │  (consumer)  │ produce   │
                    │                   └──────────────┘ topic:    │
                    │                          │  events           │
                    │                consume   ▼                   │
                    │                   ┌──────────────┐           │
                    │                   │ notification │           │
                    │                   │   server     │           │
                    │                   └──────────────┘           │
                    │   ┌───────────────────────┐                  │
                    │   │ redpanda-console :8081 │ (observability)  │
                    │   └───────────────────────┘                  │
                    └─────────────────────────────────────────────┘
```

### Components

**1. api-server** (FastAPI, listens on `:8000` inside the network)
- `POST /jobs` — accepts JSON `{"user": "alice", "action": "resize-image"}`.
  Generates a `job_id` (uuid4), produces a message to the `jobs` topic keyed by
  `user`, returns `202 Accepted` with `{"job_id": ...}`.
- `GET /health` — returns `200 {"status": "ok"}` (used by compose healthcheck).
- Holds a single module-level Kafka **producer**.

**2. worker** (plain Python long-running loop, no HTTP server)
- Kafka **consumer** subscribed to topic `jobs`, consumer group `workers`.
- On each message: logs `🔧 [worker] processing job <id> from <user> doing
  <action>`, simulates work (`time.sleep`, e.g. 1–2s), then **produces** a
  completion event to topic `events`.
- Commits offsets after producing the event (at-least-once semantics; noted in
  README as a teaching point).

**3. notification-server** (plain Python long-running loop, no HTTP server)
- Kafka **consumer** subscribed to topic `events`, consumer group `notifiers`.
- On each event: logs `🔔 [notify] Hey <user> — your job <id> (<action>) is
  done!`. In a real system this is where you'd fan out to email/SMS/push.

**4. redpanda** — single-node broker exposing the Kafka API on `9092`
(internal) and `19092` (host, for optional local experimentation). Health via
`rpk cluster health`.

**5. redpanda-console** — web UI on host `:8081`, connects to redpanda. Zero
code; observability only.

**6. nginx** — listens on `:80` (published to host as `:8080`), reverse-proxies
all requests to `api-server:8000`. Single upstream; config includes a comment
showing how you'd add replicas for load balancing.

### Topics

| Topic    | Producer     | Consumer            | Key   | Payload |
|----------|--------------|---------------------|-------|---------|
| `jobs`   | api-server   | worker              | user  | `{job_id, user, action, ts}` |
| `events` | worker       | notification-server | user  | `{job_id, user, action, status:"done", ts}` |

Topics are **auto-created** on first produce (Redpanda default). No manual
topic-creation step required; noted in README.

## Data Flow (end to end)

1. `curl -X POST localhost:8080/jobs -d '{"user":"alice","action":"resize-image"}'`
   → nginx.
2. nginx → `api-server:8000`.
3. api-server produces to `jobs`, returns `202 {job_id}`.
4. worker consumes `jobs`, logs the debug line, sleeps to simulate work,
   produces to `events`.
5. notification-server consumes `events`, logs the notification line.
6. Observe with `docker compose logs -f` and/or Redpanda Console at `:8081`.

## Repository Layout

```
backend_example/
├── docker-compose.yml
├── README.md                     # architecture + cloud-mapping + how to run
├── .env.example                  # KAFKA_BROKER, topic names, etc.
├── nginx/
│   └── nginx.conf
├── api-server/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py
├── notification-server/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── notifier.py
└── docs/superpowers/specs/
    └── 2026-07-06-scalable-backend-example-design.md
```

Each service is **fully self-contained** (its own Dockerfile, requirements, and
a small inline Kafka helper). A little duplication is intentional: it reinforces
that these are independent deployables that communicate *only* via Kafka/HTTP.

## Docker & Compose

- **3 built images:** api-server, worker, notification-server. Each Dockerfile
  is based on `python:3.11-slim`, installs its `requirements.txt`, copies its
  single source file, and sets the appropriate `CMD` (uvicorn for the API, a
  plain `python xxx.py` loop for the others).
- **3 pulled images:** redpanda, redpanda-console, nginx.
- **docker-compose.yml** wires all six containers on one bridge network.
  `depends_on` with `condition: service_healthy` makes the three Python services
  wait for redpanda to be healthy. Config (broker address, topic names) passed
  via environment variables.
- Published ports: nginx `8080:80`, redpanda-console `8081:8080`, redpanda
  `19092:19092` (optional host access).

## Error Handling (kept simple, but correct)

- **Broker not ready:** services retry the initial Kafka connection with backoff
  rather than crash-looping; `depends_on: service_healthy` covers the common
  case.
- **Consumer loops:** wrap per-message handling in try/except so one bad message
  logs an error and continues instead of killing the consumer.
- **Delivery:** producers use a delivery callback that logs failures. Worker
  commits offsets only after successfully producing the downstream event
  (at-least-once). This is documented as a deliberate teaching choice.
- **Graceful shutdown:** consumers handle SIGTERM to close the client cleanly on
  `docker compose down`.

## README Content (deliverable)

1. What this is / the flow diagram.
2. How to run (`docker compose up --build`) and a copy-paste `curl` demo.
3. How to watch it work (logs + Redpanda Console).
4. **Cloud-mapping table** answering "can I emulate the whole cloud?":

   | This project        | AWS                     | GCP                  | Azure |
   |---------------------|-------------------------|----------------------|-------|
   | nginx               | API Gateway / ALB       | Cloud LB / API GW    | App Gateway |
   | api-server          | ECS/Fargate, Lambda     | Cloud Run            | Container Apps |
   | Redpanda            | MSK / SQS               | Pub/Sub              | Event Hubs |
   | worker              | ECS worker, Lambda      | Cloud Run Jobs       | Container Apps job |
   | notification-server | SNS consumer, Lambda    | Pub/Sub push         | Functions |
   | docker-compose      | ECS task defs / EKS     | GKE                  | AKS |

5. **"What a fuller cloud sim would add"** note: MinIO (=S3), Postgres (=RDS),
   multiple api-server replicas behind nginx to demonstrate load balancing, a
   secrets store, and a metrics/tracing stack (Prometheus/Grafana/Jaeger).

## Success Criteria

- `docker compose up --build` brings up all six containers cleanly.
- The `curl` demo returns `202` and, within a couple seconds, the worker and
  notification log lines appear in `docker compose logs -f`.
- Messages are visible in Redpanda Console at `localhost:8081`.
- Each service's source file is short and readable end-to-end.
- README explains the architecture and the cloud mapping.
```
