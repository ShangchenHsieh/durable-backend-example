# Scalable Backend Example

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

## Run it

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

## How nginx routes

nginx is the only entrypoint and forwards each request by matching its path
prefix to a backend — no catch-all, so unmatched paths never reach a service:

| Request                    | Routed to        |
|----------------------------|------------------|
| `localhost:8080/api/*`     | api-server       |
| `localhost:8080/monitor`   | monitor-server   |
| anything else              | `404` from nginx |

```bash
curl localhost:8080/monitor     # → monitor-server identifies itself
curl -i localhost:8080/nope     # → 404 straight from nginx, no backend touched
```

## See the messages (Kafka)

Open **Redpanda Console** at http://localhost:8081 to watch messages land in the
`jobs` and `events` topics, and to see the three consumer groups on `events`.

## See the tasks (Celery)

Open **Flower** at http://localhost:8082 to watch Celery workers, live task flow
(received/started/succeeded/failed), task args + results, and the **Beat schedule**
with a countdown to the next `heartbeat_report`.

## What each piece is (and its cloud equivalent)

| This project        | What it does                          | AWS                 | GCP             | Azure |
|---------------------|---------------------------------------|---------------------|-----------------|-------|
| nginx               | API gateway / load balancer           | API Gateway / ALB   | Cloud LB        | App Gateway |
| api-server          | Stateless request handling            | ECS/Fargate, Lambda | Cloud Run       | Container Apps |
| Redpanda            | Durable message queue (Kafka API)     | MSK / SQS           | Pub/Sub         | Event Hubs |
| worker-server       | Async heavy IO/compute processing     | ECS worker, Lambda  | Cloud Run Jobs  | Container Apps job |
| notification-server | Event-driven fan-out to users         | SNS + Lambda        | Pub/Sub push    | Functions |
| analytic-server     | Event-driven analytics (fan-out)      | ECS/Lambda          | Cloud Run       | Container Apps |
| ai-server           | Event-driven AI enrichment (fan-out)  | SageMaker + Lambda  | Vertex + Run    | Functions |
| Redis               | Celery broker + result backend        | ElastiCache         | Memorystore     | Azure Cache for Redis |
| celery-worker       | Task-queue worker (on-demand jobs)    | ECS worker          | Cloud Run Jobs  | Container Apps job |
| celery-beat         | Periodic job scheduler                | EventBridge Scheduler | Cloud Scheduler | Logic Apps / Timer |
| Flower              | Celery monitoring UI                  | —                   | —               | — |
| docker-compose      | Local orchestration                   | ECS / EKS           | GKE             | AKS |

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
- **worker-server** uses **at-least-once** delivery (commits offsets only after
  emitting the downstream event), a deliberate, common real-world tradeoff.

## Running it for real

Three escalating ways to run this stack:

| Where | How | Directory |
|---|---|---|
| One machine | `docker compose up --build` | (this dir) |
| Local Kubernetes | `kubectl apply -f k8s/` (Docker Desktop) | [`k8s/`](k8s/) |
| **AWS (EKS)** | Terraform infra + GitHub Actions CI/CD | [`terraform/`](terraform/) + [`k8s/aws/`](k8s/aws/) |

The **AWS path** provisions a real EKS cluster with Terraform (VPC, spot nodes,
ECR, ALB Ingress via IRSA, keyless GitHub Actions OIDC) and deploys this same
pipeline onto it — cost-lean and teardownable. Start with
[`terraform/README.md`](terraform/README.md), then
[`k8s/aws/README.md`](k8s/aws/README.md).

## Project layout

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

Each service folder has its own `Dockerfile`, `requirements.txt`, source file,
and a unit test you can run with `pytest`.
