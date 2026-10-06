# FlowForge

A workflow engine that runs multi-step jobs reliably across many machines. You describe a job as a graph of steps
(call an API, query a database, build a report, wait), and FlowForge runs them in the right order, in parallel where it
can, and finishes the job even when machines crash or messages get lost.

## The problem

Business processes like "pull this month's orders, summarise them, notify finance" are chains of steps that depend on
each other. Run them with cron jobs and scripts, and one failed call, one crashed server or one duplicated message
leaves the job half-done, done twice, or silently stuck — and nobody knows which.

## Who it's for

Backend and platform teams that need background jobs to be **correct**, not just fast: every step runs exactly once in
effect, failures are retried with backoff, and every state change is recorded so an operator can see what happened
and replay what failed.

## Why now

More and more work is orchestrated across services and AI/data pipelines, and these jobs run on cheap, unreliable,
autoscaled machines. Tools like Temporal and Airflow solve this at large scale; FlowForge shows the same core
guarantees — exactly-once effects, crash recovery, ordering — built from first principles in a codebase you can read.

## Project status

**18 of 26 features done, 8 to go.** 754 automated tests pass, and the full stack has been run end to end, including
failure drills (killing and freezing workers, restarting the orchestrator, stopping Redis).

| | Features |
|---|---|
| ✅ Done | Workflow definitions and versioning · parallel DAG execution · REST API with JWT roles · HTTP, database and report steps · retries with backoff, timeouts, dead-letter queue and replay · Kafka-based workers · worker heartbeats, drain and graceful shutdown · crash recovery for lost workers and lost messages |
| ⏳ Next | Distributed locks for background jobs · pause / resume / cancel · rate limiting · dashboards and metrics · chaos endpoints · CI pipeline · benchmarks · cloud deployment |

## Tech stack

| Technology | Why |
|---|---|
| **Java 21, Spring Boot 3** | Mature, strongly typed, virtual threads for cheap concurrency. |
| **PostgreSQL** | The single source of truth; row locks and conditional updates make concurrent steps safe. |
| **Kafka** | Delivers work to many workers and survives restarts; one topic per step type routes work to capable workers. |
| **Redis** | Fast heartbeats so dead workers are detected in seconds — never the only copy of anything. |
| **Testcontainers, Docker Compose** | Tests and the local stack run against the real databases and brokers, not mocks. |

## Run it

```bash
cp .env.example .env                    # fill in the values
./mvnw verify                           # all tests (needs Docker)
docker compose up -d --build --wait     # API on http://localhost:8080, docs at /swagger-ui.html
make chaos                              # failure drills against the running stack
```
