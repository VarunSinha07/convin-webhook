# SOLUTION.md - Webhook Ingestion Service

## 1. What Was Broken, and Why

### Defect 1: Race Condition & Double-Counting (Idempotency Bug)
- **Symptom**: Duplicate call records appeared in the dashboard, and account call counts drifted higher than the actual number of calls ingested.
- **Root Cause**:
  1. `EventExists` checked for duplicate `event_id` in Postgres before inserting. Under concurrent webhook deliveries (or immediate redeliveries), multiple goroutines executed `EventExists` simultaneously, all receiving `false` before any inserted.
  2. The `events` database table lacked a `UNIQUE` constraint on `event_id`.
  3. `InsertEvent`, `UpsertCall`, and `IncrementAccountStats` executed as separate, non-atomic database queries without a transaction. Concurrent deliveries inserted multiple event rows and repeatedly incremented `account_stats` (`call_count = call_count + 1`).
  4. Redis was connected but completely unused in the ingestion hot path.

### Defect 2: Canceled Request Context in Async Goroutine & Unhandled Errors
- **Symptom**: Calls land successfully, but their call recordings are never marked processed (`recording_processed` remains `false`), and no error messages appear in logs.
- **Root Cause**:
  1. `Ingest` spawned a background goroutine `go func() { s.processRecording(ctx, rec) }()` passing `ctx` derived from HTTP request `r.Context()`.
  2. As soon as `Ingest` returned and the HTTP handler sent HTTP 200 `{"status":"accepted"}`, Go's HTTP server canceled `r.Context()`.
  3. Inside `processRecording`, `MarkRecordingProcessed(ctx, rec.CallID)` executed against Postgres using `ctx`. Because `ctx` was canceled, the Postgres query returned `context.Canceled`.
  4. The error returned by `s.processRecording` was left unhandled (`// TODO: handle`), resulting in silent failure without log output.
- **Fix**:
  1. Detached recording processing from HTTP request context cancellation using `context.WithoutCancel(ctx)` with a 10-second background processing timeout context.
  2. Added explicit error logging (`s.log.Error("recording processing failed", ...)`) if recording processing encounters an issue.

### Defect 3: Loss of In-flight Async Work During Server Graceful Shutdown
- **Symptom**: On deployment or restart, whatever async recording work was in-flight disappeared.
- **Root Cause**:
  1. In `cmd/server/main.go`, upon receiving `SIGTERM` / `SIGINT`, `srv.Shutdown(shutdownCtx)` was called to gracefully stop the HTTP server.
  2. Because the HTTP handler (`postCallWebhook`) returned HTTP 200 immediately after spawning background goroutines, zero HTTP handlers were active when `srv.Shutdown` ran.
  3. `srv.Shutdown` finished almost instantaneously, and `main()` exited immediately, terminating the process and abruptly killing background recording goroutines (`recordingWork = 50ms`) mid-execution before they could complete.
- **Fix**:
  1. Added a `sync.WaitGroup` to `ingest.Service` to track active background recording goroutines (`s.wg.Add(1)` / `defer s.wg.Done()`).
  2. Exported a `Shutdown(ctx context.Context) error` method on `ingest.Service` that blocks until all background workers finish (or until `shutdownCtx` deadline expires).
  3. Updated `cmd/server/main.go` to call `svc.Shutdown(shutdownCtx)` after HTTP server shutdown completes.

---

## 2. Deduplication Strategy & Design Defense

### Chosen Strategy: Two-Tier Deduplication (Redis Fast-Path + Postgres Durable Transaction)
1. **Tier 1 - Redis Fast-Path (`SetNX`)**:
   - On incoming webhook, `s.rdb.SetNX(ctx, "dedup:event:"+evt.EventID, "1", 24*time.Hour)` attempts an atomic key reservation with a 24-hour TTL.
   - If Redis returns `false` (key exists), the service immediately acknowledges HTTP 200 `{"status":"accepted"}` without touching Postgres or modifying stats.
2. **Tier 2 - Postgres Durable Transaction (`UNIQUE` constraint & `pgx.Tx`)**:
   - `002_add_unique_event_id.sql` adds a `UNIQUE` constraint on `events(event_id)`.
   - `IngestTransaction` executes event insertion, call upsert, and account stats increment inside a single Postgres transaction (`pgx.Tx`).
   - If Postgres returns a unique violation error (`23505`), `IngestTransaction` rolls back and returns `ErrDuplicateEvent`, which `Ingest` logs and handles gracefully as a duplicate.

### Why This Strategy Over Alternatives Considered?

| Strategy | Pros | Cons | Decision |
| :--- | :--- | :--- | :--- |
| **Postgres-Only (`SELECT` then `INSERT`)** | Simple | Prone to race conditions without serializable isolation or table locking. | ❌ Rejected |
| **Postgres `UNIQUE` Constraint Alone** | 100% durable ACID safety | Hits Postgres on every single duplicate attempt; database connection pool can saturate under heavy redelivery bursts. | ⚠️ Used as Tier 2 safety net |
| **Redis `SetNX` Alone** | Ultra-fast ($O(1)$) shielding DB | Non-durable: Redis memory eviction or restart loses deduplication state, risking duplicate counts in DB. | ⚠️ Used as Tier 1 fast-path |
| **Two-Tier (Redis Fast-Path + Postgres Constraint/Tx)** | Fast ($O(1)$ fast rejection), 100% durable ACID guarantee, zero race condition window. | Slightly more complex code. | ✅ **CHOSEN** |

---

## 3. High-Throughput Architecture (Handling 10,000 Webhooks/Second)

If this service needs to scale from modest load to **10,000 webhooks/second**, the current synchronous HTTP-to-Postgres pattern will become a database bottleneck. Here is the architectural evolution:

1. **Decouple Ingestion from Processing (Event-Driven Queue)**:
   - Webhook HTTP ingestion handler validates payload structure, performs lightweight Redis `SetNX` check, pushes the raw payload to an append-only log / message broker (**Apache Kafka**, **NATS JetStream**, or **AWS Kinesis**), and immediately returns `202 Accepted`.
   - Ingestion latency drops to $<5\text{ms}$.
2. **Partitioned Consumer Worker Pool**:
   - Background worker service consumes events partitioned by `account_id` (or `event_id`).
   - Batch database inserts (`COPY` or multi-row `INSERT`) to Postgres reduce DB transactions per second by $10\times\text{--}50\times$.
3. **Redis Cluster for Deduplication Cache**:
   - Use a sharded **Redis Cluster** or Redis Enterprise with Bloom filters / HyperLogLogs to handle millions of key lookups per second in memory.
4. **Read/Write Split & Rollup Aggregation**:
   - Instead of updating `account_stats` synchronously on every call, stream stats increments to Redis (`HINCRBY`) and flush aggregated rollups to Postgres periodically (e.g., every 5 seconds) via bulk upserts.
