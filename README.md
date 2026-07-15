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

This starts seven containers: `nginx`, `api-server`, `monitor-server`, `worker`,
`notification-server`, `redpanda` (the broker), and `redpanda-console` (a UI).

Then, in another terminal:

```bash
curl -X POST localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","action":"resize-image"}'
```

Watch the flow:

```bash
docker compose logs -f api-server worker notification-server
```

You'll see the job accepted (`📥`), processed (`🔧`), and notified (`🔔`).

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

## See the messages

Open **Redpanda Console** at http://localhost:8081 to watch messages land in the
`jobs` and `events` topics.

## What each piece is (and its cloud equivalent)

| This project        | What it does                          | AWS                 | GCP             | Azure |
|---------------------|---------------------------------------|---------------------|-----------------|-------|
| nginx               | API gateway / load balancer           | API Gateway / ALB   | Cloud LB        | App Gateway |
| api-server          | Stateless request handling            | ECS/Fargate, Lambda | Cloud Run       | Container Apps |
| Redpanda            | Durable message queue (Kafka API)     | MSK / SQS           | Pub/Sub         | Event Hubs |
| worker              | Async heavy IO/compute processing     | ECS worker, Lambda  | Cloud Run Jobs  | Container Apps job |
| notification-server | Event-driven fan-out to users         | SNS + Lambda        | Pub/Sub push    | Functions |
| docker-compose      | Local orchestration                   | ECS / EKS           | GKE             | AKS |

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
├── docker-compose.yml          # wires all seven containers together
├── .env.example                # broker address, topic names, work duration
├── nginx/nginx.conf            # the API gateway config (routes /api and /monitor)
├── api-server/                 # FastAPI: POST /api/jobs → produces to `jobs`
├── monitor-server/             # FastAPI: GET /monitor → a second routing target
├── worker/                     # consumes `jobs` → simulates work → produces `events`
└── notification-server/        # consumes `events` → logs a notification
```

Each service folder has its own `Dockerfile`, `requirements.txt`, source file,
and a unit test you can run with `pytest`.
