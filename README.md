# Distributed job scheduler

A job scheduler that guarantees exactly-once execution — even when multiple
scheduler and worker instances are running concurrently, and even if a
worker crashes mid-execution.

**Status: foundation only.** Registration API and DB wiring work. Locking,
queueing, and execution logic are not implemented yet — this is step 1 of
the build.

## Why this project exists

Most "scheduler" projects are a single cron job on a single server — that's
trivial. The actual hard problem starts the moment you run more than one
instance for reliability: how do you make sure exactly one instance
executes a given job, with no central coordinator making that decision
synchronously?

This project solves that with:
- **Distributed locking** (Redis `SET NX PX`) so concurrent scheduler
  instances don't double-claim the same job
- **Lease-based crash recovery** — locks expire automatically, so a dead
  worker doesn't permanently block a job
- **Queue-based execution** (BullMQ) decoupling "find due jobs" from
  "execute the job," so a slow target endpoint never blocks scheduling
- **Retry with exponential backoff + dead-letter queue** for jobs whose
  target endpoint fails
- **A load test** (coming later) that proves exactly-once execution by
  running multiple worker containers concurrently and killing them
  mid-execution

## Running it

```bash
cp .env.example .env
docker compose up --build
```

Then check:

```bash
curl http://localhost:3000/health
```

Register a job:

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"name": "test job", "target_url": "https://httpbin.org/post", "run_at": "2026-09-21T10:00:00Z"}'
```

## Architecture

See `docs/` (coming soon) for diagrams of the full system and the job
lifecycle from registration through execution.

## Roadmap

- [x] Project scaffold, Docker Compose, Postgres schema, health check
- [ ] Cron expression parsing + `next_run_at` calculation
- [ ] Scheduler polling loop
- [ ] Redis distributed lock with safe release (compare-and-delete via Lua)
- [ ] BullMQ queue + worker execution
- [ ] Retry with exponential backoff
- [ ] Dead-letter queue
- [ ] Missed-job recovery on startup
- [ ] Multi-container load test proving exactly-once execution
