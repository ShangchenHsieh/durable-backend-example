# Kubernetes Deployment for the Backend Example — Design

**Date:** 2026-07-07
**Author:** Shang-Chen Hsieh (with Claude)
**Status:** Approved design — implementation deferred (documented, not built)

## Goal

Take the existing docker-compose learning artifact and add a **Kubernetes
deployment** that runs the whole pipeline on a local cluster, with **2
api-server replicas and 2 worker replicas** as the headline goal. Use the move
to K8s to teach genuinely *advanced* concepts — autoscaling, disruption
budgets, health probes, config via ConfigMap, and (most importantly) how Kafka
partition count governs consumer parallelism.

Like the parent project, this is a **learning artifact**, not a production
system. Optimize for clarity and honesty over completeness. A reader should be
able to open any manifest and understand exactly what K8s object it creates and
why.

## Non-Goals

- **Not replacing docker-compose.** The compose stack stays exactly as-is. The
  K8s manifests are purely additive, in a new `k8s/` directory.
- **No Ingress, no StatefulSet, no Kustomize overlays, no separate namespace.**
  These belong to a "full platform tour" and are deliberately out of scope for
  this tier. Each is noted below as an easy extension.
- **No cloud registry / push step.** Docker Desktop's Kubernetes shares the
  Docker daemon's image store, so locally-built images are used directly.
- **No production hardening** (TLS, network policies, RBAC beyond defaults,
  persistent volumes for the broker). Called out as extensions where relevant.

## Target Environment

**Docker Desktop Kubernetes.** Chosen because:

- It shares Docker's image store — images built with `docker build` are
  immediately visible to the cluster. No `kind load` / `minikube image load` /
  registry push step. Manifests set `imagePullPolicy: IfNotPresent` so the
  cluster never tries to pull local image tags from a registry.
- `type: LoadBalancer` Services are bound to `localhost`, so `curl
  localhost:8080` keeps working exactly like it does under compose.

The one thing Docker Desktop does **not** ship is **metrics-server**, which the
HPA needs. Installing it is a single documented step (with the
`--kubelet-insecure-tls` patch Docker Desktop requires).

## Architecture

The full compose pipeline, mapped onto Kubernetes objects. Everything runs in
the `default` namespace.

```
                 Docker Desktop Kubernetes (default namespace)

  curl :8080                                                     :8081
  ────────►  Service(LoadBalancer) ──► nginx Deployment          │
   (host)         nginx :8080            │  (config from             ▼
                                         │   ConfigMap)      Service(LoadBalancer)
                                         │                    redpanda-console
                          ┌──────────────┴───────────┐
                          ▼                           ▼
                   Service: api-server         Service: monitor-server
                        :8000                        :9000
                          │                           │
              ┌───────────┴───────────┐          ┌────┴────┐
              ▼                       ▼          ▼         (1 replica)
        api-server pod          api-server pod   monitor-server pod
         (Deployment, replicas: 2, HPA 2→5, PDB minAvailable 1)
              │                       │
              └──────── produce ──────┴─► Service: redpanda :9092
                          topic: jobs          │  (redpanda Deployment,
                                               │   ClusterIP named "redpanda")
                       ┌─── consume: jobs ─────┤
                       ▼                       ▼
                 worker pod                worker pod
             (Deployment, replicas: 2, PDB minAvailable 1)
                       │                       │
                       └──── produce ──────────┴─► topic: events
                              (events)               │
                                        consume ─────┤
                                                     ▼
                                          notification-server pod
                                             (Deployment, replicas: 1)

  Job (run once): topic-init — creates `jobs` and `events` with 3 partitions
```

### Objects, by service

| Service | Deployment | Replicas | Service | Autoscale | Disruption | Probes |
|---|---|---|---|---|---|---|
| **api-server** | ✓ | **2** | ClusterIP `:8000` | **HPA CPU 2→5** | PDB `minAvailable: 1` | readiness + liveness on `GET /api/health` |
| **worker** | ✓ | **2** | — (consumer, no HTTP) | fixed (see below) | PDB `minAvailable: 1` | `exec` liveness on heartbeat file |
| notification-server | ✓ | 1 | — | fixed | — | — |
| monitor-server | ✓ | 1 | ClusterIP `:9000` | fixed | — | readiness on `/monitor` |
| nginx | ✓ | 1 | **LoadBalancer** `:8080` | fixed | — | readiness on `/` |
| redpanda | ✓ | 1 | ClusterIP `:9092`, name **`redpanda`** | fixed | — | `rpk cluster health` liveness |
| redpanda-console | ✓ | 1 | **LoadBalancer** `:8081` | fixed | — | — |
| topic-init | — (Job) | run-once | — | — | — | — |

All app Deployments carry **resource requests and limits** (required for the HPA
to compute CPU utilization) and an explicit **RollingUpdate** strategy.
api-server uses `maxUnavailable: 0, maxSurge: 1` for zero-downtime rollouts.

### Config

`.env.example` values move into a single **ConfigMap** (`00-configmap.yaml`):
`KAFKA_BROKER=redpanda:9092`, `JOBS_TOPIC=jobs`, `EVENTS_TOPIC=events`,
`WORK_SECONDS`. Deployments consume it via `envFrom.configMapRef`.

**No Secret.** This application has no credentials. A placeholder Secret would be
cargo-culting, so the design uses only a ConfigMap and adds a one-line note in
`k8s/README.md`: *in a real app, broker SASL / DB passwords would live in a
Secret referenced the same way `envFrom` references this ConfigMap.*

nginx's `nginx.conf` also becomes a ConfigMap (mounted into the nginx pod),
replacing the compose bind-mount — a clean demonstration of config-as-object.

## The two app-specific design decisions (the interesting part)

### 1. Two workers only parallelize if `jobs` has ≥ 2 partitions

The worker consumes topic `jobs` in consumer group `workers`. In Kafka, **each
partition is assigned to exactly one consumer within a group.** Topics here are
auto-created on first produce, and the dev-container default is **1 partition** —
so with 1 partition and 2 worker pods, **one worker would sit permanently
idle.** The "2 workers running" goal would be a lie.

**Fix:** a run-once **`topic-init` Job** (`20-topics-job.yaml`) using the
`redpanda` image runs `rpk topic create jobs -p 3 events -p 3
--brokers redpanda:9092`, after waiting for the broker to report healthy. Three
partitions lets both workers process in parallel with headroom.

This is the single most important manifest for making the headline goal real,
and it is the design's primary teaching moment: **partition count is the ceiling
on consumer-group parallelism.**

### 2. CPU-based HPA fits api-server, not the worker — stated honestly

- **api-server gets a real HPA** (`minReplicas: 2, maxReplicas: 5`, target ~50%
  CPU). Request handling does measurable CPU work, so a load generator
  (`hey`/`ab`, documented in the README) will drive it past 2 replicas — a
  working autoscaling demo.
- **The worker does NOT get a CPU HPA.** Its "work" is `time.sleep()` — I/O wait
  with near-zero CPU — so a CPU HPA would never trigger. Shipping one would be a
  broken demo. The worker instead stays at a fixed `replicas: 2` (the stated
  goal) plus a PDB, and `k8s/README.md` explains that the correct tool for
  consumer autoscaling is **lag-based scaling (e.g. KEDA on consumer lag)**,
  capped at the partition count (here, 3). This honesty matches the parent
  project's ethos of documenting real tradeoffs rather than hiding them.

### 3. Worker liveness without an HTTP endpoint (small code change)

The worker has no HTTP server, so it can't use an `httpGet` probe. **Decision:
option A** — the worker writes a heartbeat file (e.g. `/tmp/worker-alive`) at the
top of each poll loop, and the Deployment uses an **`exec` liveness probe** that
fails if the file is older than a threshold. This is ~4 lines added to
`worker.py` and teaches the standard non-HTTP liveness pattern (a stuck consumer
whose loop has stalled gets restarted). The alternative (no probe, restart on
crash only) was rejected as less instructive for an "advanced" artifact.

## Repository Layout (additive)

```
backend_example/
├── docker-compose.yml            # unchanged
├── worker/worker.py              # + ~4 lines: heartbeat-file write per loop
└── k8s/                          # NEW — all Kubernetes manifests
    ├── README.md                 # deploy steps, metrics-server install,
    │                             #   load-gen demo, teaching notes, extensions
    ├── build-images.sh           # docker build the 4 local images with tags
    ├── 00-configmap.yaml         # app config (broker, topics, WORK_SECONDS)
    ├── 10-redpanda.yaml          # Deployment + Service (broker, name "redpanda")
    ├── 11-redpanda-console.yaml  # Deployment + Service (LoadBalancer :8081)
    ├── 20-topics-job.yaml        # Job: rpk topic create jobs/events -p 3
    ├── 30-api-server.yaml        # Deployment(2) + Service + HPA + PDB
    ├── 40-worker.yaml            # Deployment(2) + PDB
    ├── 50-notification-server.yaml # Deployment(1)
    ├── 60-monitor-server.yaml    # Deployment(1) + Service
    └── 70-nginx.yaml             # ConfigMap(nginx.conf) + Deployment + Service(LB :8080)
```

Numeric filename prefixes suggest a natural apply order and keep the directory
readable; `kubectl apply -f k8s/` applies them all (K8s reconciles ordering via
retries regardless).

## Deploy / Demo Flow (to be documented in k8s/README.md)

1. **Enable** Kubernetes in Docker Desktop settings.
2. **Install metrics-server** (one `kubectl apply` + the `--kubelet-insecure-tls`
   patch for Docker Desktop) — required by the HPA.
3. **Build images:** `./k8s/build-images.sh` (tags the 4 local images so
   `imagePullPolicy: IfNotPresent` finds them).
4. **Deploy:** `kubectl apply -f k8s/`.
5. **Verify replicas:** `kubectl get pods` shows 2 `api-server`, 2 `worker`.
6. **Drive the pipeline:** `curl -X POST localhost:8080/api/jobs -d '{...}'`;
   `kubectl logs -l app=worker --prefix` shows *both* workers processing
   (proof the 3-partition topic parallelized them).
7. **Watch autoscaling:** run a load generator at `localhost:8080/api/jobs` and
   `kubectl get hpa -w` shows api-server scaling 2 → up to 5.
8. **Observe messages:** Redpanda Console at `localhost:8081`.

## Success Criteria

- `kubectl get pods` shows exactly **2 api-server** and **2 worker** pods
  Running.
- A batch of POSTed jobs is visibly processed by **both** worker pods (not one),
  confirming partition-based parallelism.
- Under load, `kubectl get hpa` shows api-server scaling above 2 replicas, then
  back down when load stops.
- `kubectl drain`-style voluntary disruption respects the PDBs (never takes both
  api-server or both worker pods at once).
- Every manifest is short and readable; `k8s/README.md` explains the partition
  gotcha and the worker-vs-api autoscaling distinction.
- docker-compose still works unchanged.

## Deliberate Extensions (documented, not built)

Noted in `k8s/README.md` as "where to go next," each a small, self-contained
step up:

- **Ingress** replacing the nginx Deployment (install an ingress controller).
- **StatefulSet + PVC** for Redpanda so broker data survives pod restarts.
- **Kustomize overlays** for dev/prod (e.g. different `WORK_SECONDS`, replica
  counts).
- **Dedicated namespace** (`backend-example`) instead of `default`.
- **KEDA** for lag-based worker autoscaling, capped at partition count.
- **Secret** for real credentials, referenced via `envFrom` alongside the
  ConfigMap.
```
