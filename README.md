# GoMailer — Production-Grade Bulk Email System

A high-performance concurrent bulk email delivery system built in Go using goroutines, channels, and a worker pool pattern. The system reads recipients from a CSV file, loads them into a Redis-backed persistent queue, sends emails concurrently through a single shared channel, and handles failures automatically with a retry queue and Dead Letter Queue.

---

## Overview

This is not a simple "loop and send" script. It is a reliable, crash-safe email pipeline built around Redis-persistent queues, a single buffered Go channel, and a worker pool that handles both fresh sends and retries through the same code path — no separate retry goroutines, no extra channel.

**What it does:**

- Reads recipient data from a CSV file and enqueues each entry into Redis
- Checks idempotency before enqueueing to prevent duplicate sends across runs or restarts
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

### 1. Why Redis-based queues instead of in-memory channels only?

The simplest design would be: read CSV → push into a Go channel → workers send. That works perfectly — until the application crashes halfway through a 10,000-recipient batch. Everything in-memory is gone. On restart you have no idea which emails were sent, which were not, and which were mid-flight. You either re-send everything (duplicates) or give up (data loss).

Redis solves this by being the source of truth for all queue state. Every recipient is serialized to JSON and stored in a Redis list before any worker touches it. If the app crashes, the jobs are still in Redis. On restart, the consumer picks up exactly where it left off. No duplicates, no data loss, no manual recovery needed.

Redis also gives you free monitoring — `LLEN email:queue` tells you instantly how many jobs are pending. An in-memory channel gives you nothing.

---

### 2. Why a single channel for both new emails and retries?

An earlier version of this system used two separate channels: `recipientChannel` for new jobs fed by a main consumer, and `retryChannel` for failed jobs fed by a dedicated retry consumer — each with their own worker pool.

The problem: a failed email and a new email need the exact same thing. They both need a worker to pick them up and call `SendMail()`. There is no logical reason to route them differently. Having two channels meant two consumers, two worker pools, two sets of goroutines to coordinate, and two code paths to maintain — all doing identical work.

The simplified design embeds the retry counter (`Retries int`) inside the `Recipient` struct itself. The consumer drains `email:retry` back into `email:queue` periodically, and the job flows through the exact same channel and workers as any fresh job. Workers check the counter to decide whether to retry or DLQ — that's the only difference. One channel, one consumer, one worker pool, one code path.

---

### 3. Why BLMove instead of a simple LPOP?

`LPOP` pops a job from the queue and returns it. If the application crashes between the `LPOP` and the point where the job is safely in a worker, that job is gone — it was removed from the queue but never processed.

`BLMove` is a single atomic Redis command that simultaneously pops from the source list and pushes to a destination list. The job is never in an intermediate state where it exists in neither list. If the app crashes mid-send, the job is still sitting in `email:processing` — it did not disappear. On restart, a recovery step can move everything from `email:processing` back to `email:queue` and the jobs are retried safely.

The "BL" prefix means blocking — if the queue is empty, `BLMove` waits for a new item instead of returning immediately. This means the consumer goroutine does not need a polling loop with `time.Sleep`. It simply blocks at the Redis level, consuming zero CPU, and wakes up the instant a new job arrives.

---

### 4. Why a separate `email:processing` queue?

When a consumer pops a job and hands it to a worker, there is a window of time where the job is being actively processed. During this window, two things can go wrong: the worker can fail before completing, or the application can crash entirely.

Without `email:processing`, a crashed job simply vanishes — it was popped from `email:queue` and never made it to success or retry. You would not know it existed.

`email:processing` acts as an audit trail for in-flight work. Because `BLMove` atomically moves the job there, the job is always accounted for — either in `email:queue` (waiting), `email:processing` (in-flight), `email:retry` (failed, pending retry), or `email:dlq` (permanently failed). At any point you can run `LLEN email:processing` and know exactly how many sends are happening right now. After a crash, everything in `email:processing` represents work that was interrupted and needs to be re-queued.

---

### 5. Why a Dead Letter Queue instead of just logging failures?

After 3 failed attempts, you could simply log the error and move on. The problem is that logs are not queryable, not persistent across restarts, and easy to miss. A developer has to grep through log files to find out which recipients never received their email.

The DLQ is a Redis list. It holds the full `Recipient` JSON including the original email address, name, and the final error state. You can inspect it at any time with `redis-cli LRANGE email:dlq 0 -1`, pipe it to a script for bulk re-processing, or alert on its length. When the root cause is fixed (a bad SMTP credential, a full mailbox, a temporary block), you can push DLQ items directly back into `email:queue` for another attempt — no re-importing CSVs, no manual reconstruction.

The DLQ also acts as a circuit breaker. Without it, a permanently undeliverable address (invalid domain, spam trap) would retry forever. The DLQ gives those jobs a final resting place and stops wasting SMTP quota on them.

---

### 6. Why a buffered channel with capacity 50?

The channel buffer sits between the consumer and the workers. Without a buffer (capacity 0), the consumer can only hand off one job at a time — it pushes a job, then blocks until a worker picks it up, then pushes the next. Workers and the consumer are perfectly synchronized, which means any worker that finishes early sits idle waiting for the consumer to do its next `BLMove` round-trip to Redis.

A buffered channel of capacity 50 lets the consumer run ahead of the workers. It can pull up to 50 jobs from Redis and queue them in the channel without waiting. Workers always have a job ready the moment they finish, eliminating idle time between sends.

Capacity 50 is not arbitrary — it balances two concerns. Too small and workers starve; too large and you hold too many jobs in memory, defeating the point of Redis persistence. At 5 workers each averaging under a second per send, a buffer of 50 means the consumer has roughly 10 seconds of work pre-loaded, which is enough headroom to absorb any brief Redis latency spike without stalling the workers.

---

### 7. Why SetNX for idempotency instead of checking the queue?

The naive idempotency check would be: before enqueuing, scan `email:queue` to see if this address already exists. That is an `O(n)` operation on a list — it gets slower the more items are in the queue, and it is not atomic. Two producers running in parallel could both check, both find the address absent, and both enqueue it.

`SetNX` (Set if Not Exists) is a single atomic Redis command that sets a key only if it does not already exist and returns whether it succeeded. It is `O(1)` regardless of queue size. If two producers race on the same address, only one `SetNX` can win — the other gets false and skips. There is no scan, no race condition, no extra round-trips. The key carries a 24-hour TTL so it automatically expires, allowing re-sends in a future campaign without manual cleanup.

---

## System Architecture

### High-Level Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                        SYSTEM ARCHITECTURE                        │
└──────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   CSV File   │
│ (Recipients) │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PRODUCER  (loadRecipients)                     │
│                                                                  │
│  · Read each row from CSV                                        │
│  · SetNX → email:sent:{email}   (idempotency guard)             │
│  · Marshal Recipient struct to JSON                              │
│  · RPush → email:queue          (add to RIGHT side)              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                        REDIS QUEUES                              │
│                                                                  │
│  email:queue        Main work queue  [oldest←LEFT | RIGHT→newest]│
│  email:processing   In-flight safety net (BLMove destination)    │
│  email:retry        Failed jobs waiting for another attempt      │
│  email:dlq          Dead Letter Queue — all retries exhausted    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              CONSUMER  (single goroutine)                        │
│                                                                  │
│  Loop:                                                           │
│    1. Periodically drain email:retry → email:queue  (RPush back) │
│    2. BLMove email:queue → email:processing         (atomic pop) │
│    3. Unmarshal JSON → Recipient struct                          │
│    4. Push Recipient into recipientChannel                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         recipientChannel  (single buffered channel, cap 50)      │
│                                                                  │
│      Handles BOTH new jobs and retry jobs — same channel         │
└──────┬──────────┬──────────┬──────────┬──────────┬─────────────┘
       │          │          │          │          │
       ▼          ▼          ▼          ▼          ▼
  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
  │Worker 1│ │Worker 2│ │Worker 3│ │Worker 4│ │Worker 5│
  └────┬───┘ └────┬───┘ └────┬───┘ └────┬───┘ └────┬───┘
       └──────────┴──────────┼──────────┴──────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   SMTP  (Brevo)       │
                  │     SendMail()        │
                  └──────────┬───────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
          SUCCESS                        FAILURE
              │                             │
              ▼                             ▼
  Remove from email:processing      Retries < 3 ?
                                         │
                              ┌──────────┴──────────┐
                              ▼ YES                  ▼ NO
                     Retries++               Push to email:dlq
                     Push to email:retry     Remove from processing
                     (re-enters consumer     (manual review)
                      loop on next cycle)
```

---

## Redis Queue Design

| Queue | Purpose |
|---|---|
| `email:queue` | Main work queue. Producer RPushes here. Consumer BLMoves from LEFT. |
| `email:processing` | Atomic safety net. BLMove destination while a job is being sent. Cleared on success or failure decision. |
| `email:retry` | Failed jobs. Consumer periodically moves these back into `email:queue` so they flow through the same pipeline again. |
| `email:dlq` | Dead Letter Queue. Jobs that failed all 3 attempts land here. Nothing consumes this — manual intervention only. |
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

| Attempt | What happens |
|---|---|
| Initial send (Retries = 0) | Direct from CSV load |
| 1st retry (Retries = 1) | Re-queued to `email:retry`, consumer moves to `email:queue` |
| 2nd retry (Retries = 2) | Same path |
| 3rd retry (Retries = 3) | If fails again → `email:dlq` |

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

- Go 1.18 or higher installed on your system
- Redis running locally or remotely
- SMTP credentials (from Brevo or similar SMTP provider)
- CSV file with recipient data in format: `Name, Email`

---

## Installation

```bash
go mod tidy
go get github.com/redis/go-redis/v9
go get github.com/joho/godotenv
```

This downloads the required dependencies:

- `github.com/redis/go-redis/v9` → Redis queue integration
- `github.com/joho/godotenv` → loading environment variables

---

## Configuration

Create a `.env` file in the project root:

```env
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=

SMTP_HOST=smtp-relay.brevo.com
SMTP_PORT=587
SMTP_USER=your_brevo_email@example.com
SMTP_PASS=your_brevo_smtp_password

FROM_EMAIL=noreply@yourdomain.com
FROM_NAME=Your App Name

CSV_PATH=dummy_emails.csv
WORKER_COUNT=5
MAX_RETRIES=3
CHANNEL_BUFFER=50
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

```bash
go run .
```

Or explicitly:

```bash
go run main.go producer.go consumer.go
```

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

### Frontend Dashboard
Web-based dashboard for uploading CSV files, monitoring email delivery status, viewing worker activity, and tracking campaign analytics in real time. Users will be able to manage campaigns directly from the browser without command-line interaction.

### Rate Limiter
Implement SMTP-aware request throttling to respect provider sending limits and avoid account blocking or temporary bans. Planned features include configurable emails-per-minute limits, token bucket rate limiting, and automatic exponential backoff on SMTP failures.

### Multiple Template Support
Support multiple email templates based on recipient type, campaign category, or segmentation rules. The CSV file will include a template identifier column, allowing different personalized templates to be sent within the same batch process.

### Delivery Tracking & Metrics
Add delivery metrics including:
- Success/failure counts
- Retry statistics
- Processing throughput
- Worker performance
- Queue length monitoring

### Graceful Shutdown & Recovery

Improve shutdown handling so workers finish active jobs before termination while safely persisting unprocessed jobs back into Redis for recovery on restart.
