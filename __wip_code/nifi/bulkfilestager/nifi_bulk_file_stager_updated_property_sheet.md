# NiFi Bulk-File-Stager - Updated Property Sheet

## Key confirmations
- Chunking default is ${chunk.size} with default 50000 rows per chunk (tunable).
- Manifest JSON includes manifest_path, chunk_paths, chunkCount, rows, checksum, correlation_id.
- PublishPulsar manifest batching enabled (Batch Size = 10 by default).

## Controller Services (ensure created & enabled)
- aws-credentials-provider
- pulsar-client-service
- JsonRecordSetWriter
- AutoDetectReader
- StandardSSLContextService

## Processor properties (highlights & recommended concurrency)

ListS3 (proc-lists3-bulk)
- Polling Interval: 1 min
- Batch Size: 10
- Minimum Object Age: 60 sec
- Concurrent Tasks: 1

RouteOnAttribute (proc-route-size)
- Routes:
  - large: ${s3.object.size:gt(50000000)}
  - small: ${s3.object.size:le(50000000)}
- Concurrent Tasks: 1

FetchS3Object (proc-fetchs3-bulk)
- Concurrent Tasks: 2
- Failure Strategy: penalize & retry

PutS3Object (Stage) (proc-putstage)
- Concurrent Tasks: 2-4

SplitRecord (Chunk) (proc-split-chunks)
- Record Count: ${chunk.size:toNumber():orElse(50000)} (default 50000)
- Record Reader: AutoDetectReader
- Record Writer: JsonRecordSetWriter
- Concurrent Tasks: 4

PutS3Object (Chunk) (proc-put-chunk)
- Concurrent Tasks: 4

GenerateFlowFile (Manifest) (proc-generate-manifest)
- Format: JSON with keys manifest_path, chunk_paths, chunkCount, rows, checksum, correlation_id
- Concurrent Tasks: 1

PublishPulsar (Manifest) (proc-publish-manifest)
- Topic: persistent://public/default/files.manifests
- Record Writer: JsonRecordSetWriter
- Message Key: ${correlation_id}
- Batching: true
- Batch Size: 10
- Concurrent Tasks: 2

InvokeHTTP (Airflow Trigger) (proc-invoke-airflow)
- Remote URL: http://airflow-webserver:8080/api/v1/dags/bulk_ingest/dagRuns
- Payload: {"conf":{"manifest_path":"${s3.stage.path}","correlation_id":"${correlation_id}"}}
- SSL Context Service: StandardSSLContextService
- Concurrent Tasks: 1

PublishPulsar (DLQ) (proc-dlq-bulk)
- Topic: persistent://public/default/ingest.dlq
- Concurrent Tasks: 1

## Backpressure & operational recommendations
- Back pressure thresholds: 5,000 flowfiles OR 512 MB for heavy queues.
- Chunk size tuning: start with 50k rows per chunk; reduce if DB shows contention.
- Stage bucket retention: configure lifecycle (e.g., 30-90 days)
- Airflow: ensure DAG bulk_ingest supports idempotent replays and accepts manifest_path.

---
