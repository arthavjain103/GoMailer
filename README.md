# GoMailer — Production-Grade Bulk Email System

A high-performance concurrent bulk email delivery system built in Go using goroutines, channels, and a worker pool pattern. The system reads recipients from a CSV file, loads them into a Redis-backed persistent queue, sends emails concurrently through a single shared channel, and handles failures automatically with a retry queue and Dead Letter Queue.
Web-based dashboard for uploading recipient CSV files, composing custom email templates with file attachments, managing campaigns, monitoring real-time delivery status, viewing worker activity, and tracking campaign analytics. The backend enforces a Token Bucket Rate Limiter to ensure reliable and controlled email delivery.

---

## Overview

This is not a simple "loop and send" script. It is a reliable, crash-safe email pipeline built around Redis-persistent queues, a single buffered Go channel, and a worker pool that handles both fresh sends and retries through the same code path — no separate retry goroutines, no extra channel.

**What it does:**

- Reads recipient data from a CSV file and enqueues each entry into Redis
- Checks idempotency at campaign/job level before enqueueing to prevent duplicate sends within a campaign
- Moves jobs atomically from the queue into a processing tracker using `BLMove`
- Sends emails concurrently through 5 workers sharing one buffered channel
- On failure, re-queues the job into `email:retry` with an incremented retry counter
- After 3 failed attempts, moves the job to `email:dlq` for manual review
- Survives application crashes — all state lives in Redis, not in memory

**Key numbers:**

- Handles 10,000+ recipients per batch
- 5 concurrent workers sharing a single buffered channel (capacity 50)
- Up to 3 retry attempts per recipient before Dead Letter Queue
- All queue state persists in Redis across restarts

---

## Architectural Decisions

- Redis-based queues instead of in-memory channels only
- A single shared channel for both new emails and retries
- `BLMove` instead of a simple `LPOP`
- A separate `email:processing` queue
- A Dead Letter Queue instead of only logging failures
- A buffered channel with capacity 50
- `SetNX` for campaign-scoped idempotency

---

## System Architecture

### Flow Diagram

![GoMailer flowchart](flowchart/flow.png)

---

## Redis Queue Design

| Queue                | Purpose                                                                                                                              |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `email:queue`        | Main work queue. Producer RPushes here. Consumer BLMoves from LEFT.                                                                  |
| `email:processing`   | Atomic safety net. BLMove destination while a job is being sent. Cleared on success or failure decision.                             |
| `email:retry`        | Failed jobs. Consumer periodically moves these back into `email:queue` so they flow through the same pipeline again.                 |
| `email:dlq`          | Dead Letter Queue. Jobs that failed all 3 attempts land here. Nothing consumes this — manual intervention only.                      |
| `email:sent:{email}` | Idempotency key (SetNX). Set once per address. Prevents re-enqueueing the same recipient if the app restarts or CSV is loaded twice. |

---

## Retry Logic

Each `Recipient` struct carries a `Retries int` field. The worker owns the retry decision:

1. Send attempt fails
2. Worker checks: `job.Retries < MAX_RETRIES` (MAX_RETRIES = 3)
3. **If yes:** increment counter, RPush the updated job to `email:retry`
4. **If no:** RPush to `email:dlq`, remove from `email:processing`

The consumer periodically drains `email:retry` back into `email:queue`. This keeps retries flowing through the exact same consumer → channel → worker path as new jobs.

**Retry schedule (fixed delay, no backoff):**

| Attempt                    | What happens                                                |
| -------------------------- | ----------------------------------------------------------- |
| Initial send (Retries = 0) | Direct from CSV load                                        |
| 1st retry (Retries = 1)    | Re-queued to `email:retry`, consumer moves to `email:queue` |
| 2nd retry (Retries = 2)    | Same path                                                   |
| 3rd retry (Retries = 3)    | If fails again → `email:dlq`                                |

---

## Complete Email Lifecycle

### Phase 1 — Producer

```
CSV File
  ↓
loadRecipients() reads each row
  ↓
For each recipient:
  ├─ SetNX("email:sent:john@example.com")
  │   ├─ Success (new)  → marshal to JSON, RPush to email:queue
  │   └─ Fail (exists)  → skip (already sent or already queued)
```

### Phase 2 — Consumer + Workers

```
Consumer goroutine (single):
  ├─ Drain email:retry → email:queue  (periodically)
  └─ BLMove email:queue → email:processing
       ↓
  Push to recipientChannel

Worker 1-5 (reading from same channel):
  ├─ Render email from template
  ├─ SendMail() via SMTP
  │   ├─ SUCCESS → removeFromProcessing()
  │   └─ FAILURE:
  │       ├─ Retries < 3 → Retries++, RPush to email:retry
  │       └─ Retries ≥ 3 → RPush to email:dlq
  └─ removeFromProcessing()
```

### Phase 3 — Completion

```
email:queue      → empty
email:processing → empty
email:retry      → empty
email:dlq        → contains any permanent failures (review manually)
WaitGroup drains → all workers done → application exits
```

---

## Idempotency

If a CSV has duplicate entries, or if the application restarts after a partial run, the same email will not be enqueued twice.

```go
key := "email:sent:" + recipient.Email
set, err := rdb.SetNX(ctx, key, 1, 24*time.Hour).Result()

if !set {
    // Key already exists — skip this recipient
    continue
}
```

`SetNX` is atomic in Redis. The first time an address is seen it succeeds and the job is enqueued. Every subsequent call for the same address within 24 hours returns false and is skipped silently.

---

## FIFO Queue Processing

Jobs are processed in the order they were enqueued — oldest first.

**Producer (RPush adds to the RIGHT):**

```
email:queue: [LEFT] John → Jane → Bob [RIGHT]
```

**Consumer (BLMove pops from the LEFT):**

```
After one BLMove:
  email:queue:      [LEFT] Jane → Bob [RIGHT]
  email:processing: [John]
```

This guarantees no recipient waits indefinitely and the batch completes in a predictable, fair order.

---

## Monitoring

Check queue depths live with Redis CLI while the application is running:

```bash
# How many emails waiting to send?
redis-cli LLEN email:queue

# How many currently in-flight?
redis-cli LLEN email:processing

# How many waiting for retry?
redis-cli LLEN email:retry

# How many permanently failed?
redis-cli LLEN email:dlq

# Was this address already processed?
redis-cli EXISTS email:sent:john@example.com

# Inspect DLQ contents
redis-cli LRANGE email:dlq 0 -1
```

## Troubleshooting

**"Error connecting to Redis"**

```bash
redis-cli PING   # should return: PONG
redis-server     # start if not running
```

**Emails stuck in `email:processing` after a crash**

```bash
redis-cli LRANGE email:processing 0 -1   # inspect first
redis-cli DEL email:processing            # clear it
# Re-run the application — consumer will re-enqueue on next start
```

**Too many items in `email:dlq`**

```bash
redis-cli LRANGE email:dlq 0 -1   # inspect failure reasons in logs
# Fix root cause (bad SMTP credentials, invalid addresses, etc.)
# Reset Retries to 0, RPush back to email:queue, re-run
```

**Application won't exit**

```bash
redis-cli LLEN email:retry   # check if retry queue is stuck
redis-cli LLEN email:queue   # check main queue
# If stuck, inspect logs for repeated SMTP errors
```

---

## Project Structure

```
go-email-sender/
├── main.go              # Entry point — initializes workers, WaitGroup, template
├── producer.go          # CSV reader — parses recipients and enqueues to shared
├── consumer.go          # Email worker — SMTP sending, error logging, retry
├── email.tmpl           # Email template with {{.Name}} and {{.Email}}
├── dummy_emails.csv     # Sample recipient data for testing
├── .env                 # SMTP credentials and configuration (not pushed to git)
├── go.mod               # Go module definition
└── go.sum               # Dependency checksums
```

---

## Setup Instructions

### Prerequisites

- Docker Desktop / Docker Engine installed
- Go 1.18+ if you want to run without Docker
- SMTP credentials from Brevo or any compatible SMTP provider
- CSV file with recipient data in format: `Name, Email`

---

## Docker Setup (Recommended)

This project is configured to run with Redis and PostgreSQL through Docker Compose.

### 1) Create environment file

Use the project root `.env` file with values like:

```env
SMTP_USER=your_smtp_username
SMTP_PASSWORD=your_smtp_password
smtpHost=smtp-relay.brevo.com
smtpPort=587
SMTP_FROM=you@example.com
POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/gomailer?sslmode=disable
```

### 2) Start the services

```bash
docker compose up -d
```

This starts:

- Redis on `localhost:6379`
- PostgreSQL on `localhost:5432`
- GoMailer app on `http://localhost:8080`

### 3) Check the running containers

```bash
docker ps
```

### 4) View app status

```bash
curl http://localhost:8080/api/status
```

### 5) Stop services

```bash
docker compose down
```

To remove database volume as well:

```bash
docker compose down -v
```

---

## Local Go Run (Optional)

If you want to run the Go app without Docker containers:

```bash
go mod tidy
go run .
```

Make sure Redis and PostgreSQL are already running locally.

---

## Installation

```bash
go mod tidy
go get github.com/redis/go-redis/v9
go get github.com/joho/godotenv
go get github.com/jackc/pgx/v5/stdlib
```

This downloads the required dependencies:

- `github.com/redis/go-redis/v9` → Redis queue integration
- `github.com/joho/godotenv` → environment configuration
- `github.com/jackc/pgx/v5/stdlib` → PostgreSQL driver

---

## Configuration

The app reads settings from `.env` and Docker Compose environment variables.

### Redis details

```env
REDIS_ADDR=localhost:6379
```

---

## Prepare CSV File

Create a CSV file with recipients in format:

```csv
Name,Email
John Doe,john@example.com
Jane Smith,jane@example.com
Bob Wilson,bob@example.com
```

---

## Running the Application

With Docker:

```bash
docker compose up -d
```

Without Docker:

```bash
go run .
```

Then upload a CSV from the dashboard or use the campaign API and start sending.

---

## Sample Output

```bash
Connected to Redis: PONG

Enqueued: john@example.com
Enqueued: jane@example.com
Enqueued: bob@example.com

Worker 1: sending to john@example.com
Worker 2: sending to jane@example.com
Worker 3: sending to bob@example.com

Worker 1: sent john@example.com successfully
Worker 2: failed jane@example.com (attempt 1/3) — queued for retry
Worker 3: sent bob@example.com successfully

Worker 1: retrying jane@example.com (attempt 2/3)
Worker 1: sent jane@example.com successfully


---


## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go (Golang) |
| Concurrency | Goroutines, buffered channels, `sync.WaitGroup` |
| Queue / persistence | Redis (Lists + SetNX) |
| SMTP provider | Brevo (standard SMTP compatible) |
| Email templating | Go `text/template` |
| Configuration | Environment variables via `godotenv` |
| Data input | CSV files |

---

# Future Enhancements

## Current Focus Areas

### Multiple Template Support
Support multiple email templates based on recipient type, campaign category, or segmentation rules. The CSV file will include a template identifier column, allowing different personalized templates to be sent within the same batch process.
```
