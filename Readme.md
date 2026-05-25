# Distributed Task Queue Engine

![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

A production-grade distributed task queue engine built in Go — supporting priority scheduling, fault-tolerant retries, dead-letter queues, watchdog-based crash recovery, and full observability.

---

## Benchmarks

| Metric | Result |
|---|---|
| Total task submissions | 200,000+ |
| Throughput | ~6,700 req/sec |
| HTTP failure rate | 0% |
| p95 latency | ~30ms |

> Measured under sustained k6 load testing across 3 concurrent worker containers.

---

## Features

- **Priority queues** — high, medium, low via Redis sorted sets
- **Exponential backoff retries** — 2s → 4s → 8s per failure
- **Dead-letter queue** — isolates tasks exceeding retry limits
- **Watchdog recovery** — detects and requeues stale tasks after worker crashes
- **At-least-once delivery** — no task is silently dropped
- **PostgreSQL persistence** — full task lifecycle audit log
- **Prometheus + Grafana** — real-time observability dashboards
- **Docker Compose** — entire stack spun up in one command

---

## Architecture

```
+------------------+
|    HTTP API      |
|   (Go Server)    |
+------------------+
          |
          v
+------------------+
|      Redis       |
| Priority Queues  |
+------------------+
   |      |      |
   v      v      v
+-------+ +-------+ +-------+
|  W1   | |  W2   | |  W3   |
+-------+ +-------+ +-------+
          |
          v
+------------------+
| Retry Scheduler  |
+------------------+
          |
          v
+------------------+
|  Dead Letter Q   |
+------------------+

+------------------+     +------------------+
|   PostgreSQL     |     |   Prometheus     |
+------------------+     +------------------+
                                  |
                                  v
                         +------------------+
                         |     Grafana      |
                         +------------------+
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| Queue Backend | Redis |
| Database | PostgreSQL |
| Monitoring | Prometheus |
| Visualization | Grafana |
| Containerization | Docker |
| Load Testing | k6 |

---

## Task Lifecycle

```
Pending → Processing → Completed
                ↓
             Failed
                ↓
            Retry Queue (exponential backoff)
                ↓
          Dead Letter Queue (max retries exceeded)
```

---

## Prometheus Metrics

| Metric | Description |
|---|---|
| `task_queue_depth` | Current tasks waiting in queue |
| `tasks_processed_total` | Total tasks successfully completed |
| `tasks_failed_total` | Total tasks that failed execution |
| `task_retry_count_total` | Total retry attempts across all tasks |
| `dead_letter_queue_size` | Tasks in the dead-letter queue |
| `task_processing_duration_seconds` | Task execution duration histogram |

---

## Project Structure

```
Distributed-task-queue-engine/
│
├── cmd/
│   ├── server/
│   └── worker/
│
├── internal/
│   ├── api/
│   ├── handlers/
│   ├── metrics/
│   ├── models/
│   ├── queue/
│   ├── retry/
│   ├── storage/
│   ├── watchdog/
│   └── worker/
│
├── deployments/
│   └── prometheus.yml
│
├── tests/
│   └── loadtest.js
│
├── docker-compose.yml
├── Dockerfile.server
├── Dockerfile.worker
├── go.mod
└── README.md
```

---

## Getting Started

### Start all services

```bash
docker compose up --build
```

### Submit a task

```bash
curl -X POST http://127.0.0.1:8081/tasks \
  -H "Content-Type: application/json" \
  -d '{"name":"send_email","payload":"hello","priority":1,"max_retries":3}'
```

### Run load test

```bash
k6 run tests/loadtest.js
```

### Access dashboards

| Service | URL | Credentials |
|---|---|---|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
