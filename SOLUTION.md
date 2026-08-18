# SOLUTION.md - Webhook Ingestion Service

## Overview & Architecture Summary
This document provides a detailed breakdown of the three production defects identified in `webhook-ingest`, the architectural rationale behind our deduplication design, and a roadmap for scaling the service to 10,000 webhooks/second.

---

## 1. What Was Broken, and Why?

### Defect 1: Race Condition in Deduplication & Stats Double-Counting
- **Ops Symptom**: Duplicate call records appeared in the dashboard, and per-account call counts drifted higher than actual calls.
- **Code Location**: `internal/ingest/service.go` (`Ingest`) and `internal/store/store.go` (`InsertEvent`, `IncrementAccountStats`).
- **Root Cause**:
  1. **Non-Atomic Check-Then-Act**: `EventExists` checked Postgres before inserting. Under concurrent webhook retries (or parallel HTTP requests with identical `event_id`), multiple goroutines queried `EventExists` simultaneously. All returned `false` before any single goroutine executed `InsertEvent`.
  2. **Missing DB Uniqueness Constraint**: The `events` database schema in `001_init.sql` indexed `event_id` but lacked a `UNIQUE` constraint. Postgres accepted duplicate `event_id` rows without error.
  3. **Non-Transactional Writes**: `InsertEvent`, `UpsertCall`, and `IncrementAccountStats` were executed as separate SQL queries outside an ACID database transaction. Every duplicate request executed `account_stats.call_count + 1`, causing account stats to drift upwards indefinitely.
  4. **Unused Redis Instance**: Redis was initialized in `main.go` but never used in the ingestion path.
- **How We Fixed It**:
  1. **Schema Migration (`002_add_unique_event_id.sql`)**: Added `UNIQUE (event_id)` constraint on `events`.
  2. **Postgres ACID Transaction (`store.IngestTransaction`)**: Combined event insert, call upsert, and account stats increment into a single `pgx.Tx` transaction. If `unique_event_id` is violated (Postgres error code `23505`), `IngestTransaction` rolls back completely and returns `store.ErrDuplicateEvent`.
  3. **Redis Atomic Fast-Path (`rdb.SetNX`)**: In `Ingest`, added `SetNX(ctx, "dedup:event:"+evt.EventID, "1", 24*time.Hour)`. If Redis key already exists, `Ingest` returns `nil` (HTTP 200 `accepted`) in $<1\text{ms}$ without querying Postgres.
- **Verification**: `TestConcurrentDuplicateDeliveriesDoesNotDoubleCount` fires 10 parallel goroutines with the same payload; exactly 1 event is stored and `account_stats.call_count` equals 1.

---

### Defect 2: Canceled Request Context in Async Goroutine & Silent Failures
- **Ops Symptom**: Webhook calls landed successfully, but call recordings were never marked as processed (`recording_processed` stayed `false`), and no logs were produced.
- **Code Location**: `internal/ingest/service.go` (`processRecording`).
- **Root Cause**:
  1. `Ingest` launched a background goroutine `go func() { s.processRecording(ctx, rec) }()` passing `ctx` from `r.Context()`.
  2. `postCallWebhook` handler immediately returned HTTP 200 OK. Standard Go `http.Server` cancels `r.Context()` the moment the HTTP handler returns.
  3. Inside `processRecording`, `s.store.MarkRecordingProcessed(ctx, rec.CallID)` ran after a `50ms` simulated delay (`recordingWork`). Because `ctx` was already canceled, `pgxpool` aborted the query immediately with `context.Canceled`.
  4. The error return was left unhandled (`// TODO: handle`), swallowing the error silently.
- **How We Fixed It**:
  1. **Detached Context**: Used `context.WithoutCancel(ctx)` with a dedicated background timeout (`context.WithTimeout(..., 10*time.Second)`).
  2. **Explicit Logging**: Logged any processing error with `s.log.Error("recording processing failed", ...)`.
- **Verification**: `TestRecordingProcessedEvenWhenContextCanceled` cancels the HTTP request context immediately after sending the POST request and asserts that `recording_processed` is set to `true` in Postgres.

---

### Defect 3: In-Flight Async Work Disappearing on Deployment / Restart
- **Ops Symptom**: Every time the service redeployed, whatever async recording work was currently in-flight disappeared.
- **Code Location**: `cmd/server/main.go` and `internal/ingest/service.go`.
- **Root Cause**:
  1. `main.go` caught `SIGTERM`/`SIGINT` and invoked `srv.Shutdown(shutdownCtx)`.
  2. `srv.Shutdown` waits only for active HTTP handlers to return. Because `postCallWebhook` returned HTTP 200 in $<1\text{ms}$ (spawning goroutines out-of-band), zero HTTP handlers were active during shutdown.
  3. `srv.Shutdown` returned instantly, `main()` reached end of function, and the process exited—killing all background goroutines sleeping or writing to DB mid-execution.
- **How We Fixed It**:
  1. **`sync.WaitGroup` Tracking**: Added `wg sync.WaitGroup` to `ingest.Service`. `s.wg.Add(1)` is called before spawning a worker goroutine, and `defer s.wg.Done()` runs on completion.
  2. **`Service.Shutdown(ctx)`**: Implemented `Shutdown(ctx)` which blocks on `s.wg.Wait()` or until `shutdownCtx` deadline expires.
  3. **Main Wiring**: Updated `cmd/server/main.go` to call `svc.Shutdown(shutdownCtx)` after `srv.Shutdown(shutdownCtx)`.
- **Verification**: `TestServiceShutdownWaitsForInFlightRecordings` ingests a webhook, triggers `svc.Shutdown()` immediately, and verifies the recording is processed before shutdown completes.

---

## 2. Deduplication Strategy & Design Defense

### The Two-Tier Strategy Explained
We adopted a **Two-Tier Deduplication Pattern**:

```
           [ Webhook Incoming ]
                    │
                    ▼
     ┌──────────────────────────────┐
     │ Tier 1: Redis Fast-Path      │ ── (Key Exists) ──► Return 200 OK (Accepted)
     │ SetNX("dedup:event:<id>")    │                     [< 1ms Response Time]
     └──────────────┬───────────────┘
                    │ (Key Reserved)
                    ▼
     ┌──────────────────────────────┐
     │ Tier 2: Postgres ACID Tx     │ ── (Unique Violation 23505) ──► Rollback & Return 200 OK
     │ UNIQUE (event_id)            │
     └──────────────┬───────────────┘
                    │ (Tx Committed)
                    ▼
     [ Update Stats & Process Async ]
```

### Alternatives Considered & Trade-off Analysis

| Strategy | Pros | Cons | Decision Rationale |
| :--- | :--- | :--- | :--- |
| **1. Application `SELECT` check before `INSERT`** | Simple to write | Complete vulnerability to race conditions under concurrent retries; zero isolation guarantees without `SERIALIZABLE` transactions. | ❌ **Rejected**: Root cause of original bug. |
| **2. Postgres `UNIQUE` Constraint Alone** | 100% durable ACID safety guarantee | Every duplicate retry forces a full network roundtrip + DB write lock contention in Postgres. Under heavy retry storms, connection pool (`DBMaxConns`) saturates. | ⚠️ **Partial**: Kept as Tier 2 safety net. |
| **3. Redis `SetNX` Alone** | Extremely fast ($O(1)$), $<1\text{ms}$ latency, shields DB | Non-durable: If Redis restarts, evicts keys under memory pressure, or fails over, deduplication history is lost, leading to DB state corruption. | ⚠️ **Partial**: Kept as Tier 1 fast path. |
| **4. Two-Tier (Redis Fast-Path + Postgres ACID Tx)** | Fast path rejects $>99\%$ retries in memory; DB transaction guarantees $100\%$ durability even if Redis crashes or evicts. | Requires managing two data stores in service logic. | ✅ **Selected Strategy**: Best balance of speed and safety. |

---

## 3. High-Throughput Architecture (10,000 Webhooks/Second)

To scale from single-digit webhooks/sec to **10,000 webhooks/sec**, the synchronous HTTP-to-Postgres architecture must evolve to a fully decoupled, stream-processed architecture.

```
 [ Telephony Webhooks ] (10,000 req/sec)
           │
           ▼
 ┌───────────────────┐
 │ API Ingestion     │ (Stateless Go Nodes)
 │ - JSON Validate   │
 │ - Redis Fast-Path │
 └─────────┬─────────┘
           │ (Publish Payload in < 2ms)
           ▼
 ┌───────────────────┐
 │ NATS / Kafka      │ (Partitioned Log Stream by account_id)
 └─────────┬─────────┘
           │
           ▼
 ┌───────────────────┐
 │ Worker Pool       │ (Batch Processing Workers)
 │ - Micro-batching  │ (100 events / batch -> COPY to Postgres)
 └─────────┬─────────┘
           │
           ▼
 ┌───────────────────┐
 │ Postgres / Redis  │ (Postgres Read Replicas + Redis Cluster)
 └───────────────────┘
```

### 4 Key Architectural Changes for 10k/sec:

1. **Decouple Ingestion from Persistence (Message Broker)**:
   - Replace direct DB writes in HTTP handler with an event streaming platform like **Kafka**, **NATS JetStream**, or **AWS Kinesis**.
   - Ingestion HTTP node validates JSON, performs Redis `SetNX`, publishes event to Kafka topic, and responds `202 Accepted` in $<3\text{ms}$.
2. **Micro-Batching Database Writes**:
   - Instead of $10,000$ individual SQL transactions/sec against Postgres, consumer workers read batches of events (e.g. 500 events every 50ms) and perform bulk inserts (`COPY` or multi-row `INSERT ON CONFLICT`). This reduces DB write operations by $100\times\text{--}500\times$.
3. **Distributed Redis Cluster & HyperLogLog / Bloom Filters**:
   - Use a sharded **Redis Cluster** for deduplication keys, or use **Cuckoo / Bloom Filters** in Redis to check millions of `event_id` entries with minimal memory footprint ($~1\text{ byte per event}$).
4. **Out-of-Band Stats Aggregation (Write-Behind Cache)**:
   - Stream `account_stats` increments to Redis (`HINCRBY`), and run a periodic background worker to flush aggregated account totals to Postgres `account_stats` every 5 seconds.

---

## 4. Cheat Sheet for the 30-Minute Code Walkthrough

If the interviewer asks:
- **"What happens if Redis goes down?"**
  - Our code handles Redis errors gracefully (`s.log.Warn("redis deduplication check failed...")`) and falls back seamlessly to Postgres `IngestTransaction`. Postgres `UNIQUE` constraint guarantees data correctness even during total Redis outage.
- **"Why did you use `context.WithoutCancel` instead of `context.Background()`?"**
  - `context.WithoutCancel` preserves context values (like trace IDs, spans, logging metadata) attached to the original HTTP context while detaching from the HTTP cancellation signal.
- **"What happens if the service crashes mid-recording download?"**
  - Currently `recording_processed` stays `FALSE`. In a production system, a background reconciliation cron job would query `SELECT call_id FROM calls WHERE recording_processed = FALSE AND updated_at < NOW() - INTERVAL '5 minutes'` and re-queue them.
