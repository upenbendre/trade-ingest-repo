# Trade Ingestion Reference Architecture (Real-time + Batch)  
A concise reference architecture showing how front-office (real-time) and batch/bulk trade sources converge into a single canonical **Trade Store**. This document contains a diagram, step-by-step pipelines, data model recommendations, transactional/idempotency approaches, failure-handling patterns, and concise pseudocode examples.

---

## 1) High-level diagram
```
                                External Venues
                       (Exchanges, Broker Feeds, FIX Gateways)
                                        |
                 -------------------------------------------------
                 |                                               |
            Streaming                                      Batch/Files
         (Kafka / MQ / FIX)                                (S3 / FTP / DB Exports)
                 |                                               |
         +-------+------+                                +-------+--------+
         |              |                                |                |
   Stream Normalizer  Stream Processor              File Ingestor     Batch ETL
   (schema, enrich)   (Flink / Kafka Streams)         (Spark/Flink)     (Spark / Airflow)
         |              |                                |                |
         +-------+------+                                +-------+--------+
                 |                                               |
        Event bus (topic for canonical trades, compacted)   Batch staging (parquet/json)
                 |                                               |
           Real-time dedupe / txn logic                 Batch job calculates upserts
                 |                                               |
                 +--------------------+--------------------------+
                                      |
                         Convergence Layer / Ingest API / CDC
                                      |
                        Idempotent Upsert / Transactional Sink
                                 (Distributed SQL DB / Ledger)
                                      |
                                  Trade Store
                               (canonical trade table)
                                      |
                Downstream: Risk, P&L, Reconciliations, Settlements
```

---

## 2) Two pipelines — overview
- **Real-time pipeline**: low-latency, per-trade messages via Kafka (or MQ). Intended for intraday risk, real-time P&L, alerts.
  - Characteristics: single-trade messages, small payloads, event timestamps, schema-registered (Avro/Protobuf/JSON), high throughput.
  - Components: FIX gateway -> normalizer -> Kafka topic(s) -> stream processor (Flink/Kafka Streams) -> transactional upsert sink.

- **Batch pipeline**: bulk file drops or DB extracts (S3/FTP/DB). Intended for reconciliation, backfills, large EOD loads.
  - Characteristics: large files with many trades, possibly partial duplicates of real-time messages, richer fields (allocations), higher latency (minutes-hours).
  - Components: file landing -> file ingestor (Spark/Flink batch job) -> staging -> transform -> upsert into same trade store.

**Key point:** both pipelines write into the *same canonical trade table* (trade_store). They must obey the same business rules (e.g., maturity-date-based accept/reject), be idempotent, and preserve ordering/last-wins semantics.

---

## 3) Canonical data model & keys
- `trade_id` (primary business key) — must be globally unique per trade lifecycle.
- `version` or `sequence_no` — integer or logical timestamp used for last-writer-wins.
- `source_system` — origin (OMS, broker, file, etc.).
- `received_at` (ingest timestamp) and `trade_timestamp` (trade event time).
- `maturity_date` — used for business acceptance rules.
- `status`/`lifecycle_stage` — booked/affirmed/cleared/expired.
- `payload`/normalized columns — instrument, quantity, price, counterparty, etc.

Recommendation: ensure **trade_id + (optional) leg_id** uniqueness and include a monotonic `source_sequence` or `last_modified_at` timestamp. If upstream can't provide sequence, use `received_at` plus `source_system` and a hashing strategy.

---

## 4) Business rule example: maturity-date validation
**Rule**: If an incoming record for existing `trade_id` has a `maturity_date` older (earlier) than the latest accepted maturity in the trade store, **reject** the record and do not update the store.

Implementation approaches:
- Upsert with conditional WHERE: `UPDATE trade_store SET ... WHERE trade_id = ? AND incoming_maturity >= existing_maturity` (or use an expression comparing `version`/`last_modified_at`).
- Use a compare-and-swap (CAS) operation or database-side stored proc to atomically apply the rule.

---

## 5) Idempotency and deduplication strategies
1. **Idempotent upserts**: consumer transforms each trade into an UPSERT (or MERGE) keyed by `trade_id`. Upserts must include logic to ignore older versions.

2. **Dedup store**: a small local key-value store (RocksDB in stateful Flink, or a DB table) that stores seen `(trade_id, message_id/sequence)` for a TTL. Use it to dedupe near real-time duplicates.

3. **Compacted Kafka topics**: publish canonical trades to a compacted topic keyed by `trade_id`. Consumers can replay the compacted topic to rebuild state; producers can write idempotently.

4. **Source message identifiers**: ensure each message contains `message_id` or `(source, sequence_no)` so you can detect replays.

---

## 6) Transactionality & exactly-once semantics
Options (ordered by practical robustness):

1. **Stream processor with two-phase commit sink** (Flink’s 2PC sink or Kafka Connect’s transactional connectors):
   - Achieve end-to-end exactly-once from Kafka to JDBC-compatible DB if connector supports 2PC.
   - Good for real-time per-record correctness.

2. **Kafka transactions + idempotent upsert in DB**:
   - Use Kafka transactions to atomically write consumed offsets and produced messages; sink performs idempotent upserts (MERGE) in DB.

3. **Outbox + CDC**:
   - Write canonical event to DB first (transactional outbox), then use CDC (Debezium) to stream changes back to Kafka for downstream consumers. This gives DB-first durability and simplifies external visibility.

4. **Batch atomic upserts**:
   - For batch jobs, write to a staging table and then run a single atomic DB `MERGE` (transaction) into the canonical table.

**Important**: Not all distributed SQL DBs support the same transactional guarantees — choose an approach based on your trade store DB. Flink 2PC sink + a DB that supports XA or transactional writes is a strong choice for banks.

---

## 7) Example pseudocode (real-time consumer using Flink-like logic)
```
for each message in kafka_topic:
    canonical = normalize(message)
    if is_duplicate(canonical):
        ack and continue
    if canonical.trade_id exists in store:
        existing = read_store(canonical.trade_id)
        if canonical.version < existing.version:   # or compare maturity
            ack and continue
    # atomic upsert (DB should ensure conditional update)
    db.upsert_trade(canonical)  # MERGE with WHERE version >= incoming
    ack
```

For Flink you would use a 2PC sink so the checkpoint guarantees ensure offsets are committed only after DB commit.

---

## 8) Example SQL MERGE (generic)
```sql
MERGE INTO trade_store t
USING (SELECT :trade_id AS trade_id, :version AS version, :maturity_date AS maturity_date, :payload AS payload) src
ON t.trade_id = src.trade_id
WHEN MATCHED AND src.version >= t.version AND src.maturity_date >= t.maturity_date
  THEN UPDATE SET version = src.version, maturity_date = src.maturity_date, payload = src.payload, last_modified = CURRENT_TIMESTAMP
WHEN NOT MATCHED
  THEN INSERT (trade_id, version, maturity_date, payload, created_at) VALUES (src.trade_id, src.version, src.maturity_date, src.payload, CURRENT_TIMESTAMP);
```
This enforces last-wins based on `version` and also verifies maturity condition.

---

## 9) Reconciliation & convergence checks
- Run periodic recon jobs that compare compacted-topic state vs trade_store vs batch source snapshots. Report mismatches.
- Keep an **Affirm/Confirm** pipeline: confirmations from middle-office or counterparties should be matched to the trade_store and drive status updates.

---

## 10) Operational concerns
- **Latency**: Real-time path should be <seconds to satisfy intraday needs; batch path measured in minutes.
- **Throughput**: Kafka + Flink can handle many thousands/sec; batch jobs should be parallelized.
- **Backpressure & spikes**: use topic partitioning, autoscaling stream processors, and buffer to S3 if downstream DB is slow.
- **Monitoring**: metrics for lag, failed upserts, duplicate counts, rejections due to maturity rules.
- **Backfill**: replay compacted topic or run batch-only MERGE to rebuild state.

---

## 11) Recommended tech stack (example)
- Streaming: Kafka (schema-registry + compacted topics), or Solace/MQ for legacy.
- Processing: Flink (stateful streaming + two-phase commit), Kafka Streams for simpler topologies.
- Batch: Spark or Flink batch, orchestrated by Airflow/Argo.
- Trade store: Distributed SQL (Yugabyte/Cockroach) or highly available RDBMS (Postgres/Oracle) with good transactional support.
- CDC: Debezium for outbox/CDC patterns.

---

## 12) Short checklist before implementation
- Ensure upstream provides a stable `trade_id` and a monotonic `version` or `last_modified` where possible.
- Define canonical schema & schema evolution policy in schema registry.
- Decide E2E transactional model (Flink 2PC, outbox+CDC, or idempotent MERGE).
- Implement conditional MERGE logic to enforce maturity/version rules.
- Add dedupe and duplicate-detection with TTL window.
- Create monitoring and reconciliation jobs.

---

If you'd like, I can now:
- produce a **Mermaid visual** of the diagram; or
- provide an **example Flink job** in Python/Scala that implements the real-time path with a 2PC JDBC sink (pseudocode or runnable); or
- draft the **DB schema** (DDL) for `trade_store` and a sample `MERGE` stored-procedure for your target DB (Yugabyte/Cockroach/Postgres/Snowflake).

Which next artifact would you like me to create?

