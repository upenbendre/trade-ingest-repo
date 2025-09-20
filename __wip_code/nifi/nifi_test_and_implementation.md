
Below is what needs to be done for a production ready NiFi import:

1. NiFi version compatibility: Confirm NiFi version (e.g., 1.18/1.19+) and ensure NARs (Pulsar, AWS, record processors) in the template match your NiFi release.
2. Controller Services & Credentials: Create & enable controller services referenced in the template: AWS Credentials Provider, PulsarClientService, Record Readers/Writers, SSL context, Distributed Map Cache if used.
3. Populate sensitive properties securely (AWS keys, Airflow tokens, Pulsar auth).
4. NARs / Extensions: Install required NARs: nifi-pulsar-nar, nifi-aws-nar, nifi-record-nar, nifi-json-nar etc. Ensure versions compatible with NiFi.
5. Schema Registry: Ensure schema registry endpoint is reachable and schemas (trade-canonical-schema, bulk-manifest-schema, trade-csv-schema) are registered.
6. Controller service IDs & UUIDs: When importing template, map controller service IDs to actual services in your NiFi instance (the scaffold uses placeholder IDs).
7. Cluster setup: If NiFi runs in cluster mode, ensure all nodes have access to NARs and credentials; enable site-to-site/cluster communication.
8. Security: Configure TLS for NiFi and for downstream services (Pulsar, S3, Airflow). Use NiFi's StandardSSLContextService for HTTPS calls.
9. Enable NiFi authentication (LDAP/OAuth/PKI) and RBAC for process group access.
10. Airflow connectivity: Ensure Airflow REST API endpoint is reachable from NiFi; obtain an API token and store it as a sensitive property; confirm DAG bulk_ingest exists and accepts manifest_path.
11. S3 buckets & permissions: Create landing, stage, chunk, archive and DLQ buckets with correct IAM policies. Ensure NiFi's AWS credentials have needed permissions (List/Get/Put/Delete).
12. Pulsar topics & quotas: Pre-create Pulsar namespaces/topics if needed; set quotas for bulk namespace (publishRateInMsg, publishRateInByte). Ensure PulsarClientService is configured with correct service URL and auth.
13. Disk & provenance sizing: NiFi provenance and content repositories require disk. Estimate and allocate storage for expected retention (e.g., 30 days). Configure repository retention/cleanup.
14. Back-pressure & queue tuning: Set connection-level back-pressure thresholds (flowfile count & size) and per-processor concurrent tasks to prevent resource exhaustion.
15. Chunk-size & chunking policy: Decide default chunk size (start 50k rows) and tune against DB ingest throughput. Configure chunk naming conventions and lifecycle.
16. DLQ & replay process: Configure DLQ retention (S3 + Pulsar) and create a replay process group for manual/automated re-ingestion.
17. Monitoring & alerts: Hook NiFi metrics into Prometheus/Grafana (Prometheus Reporting Task). Create alerts for queue depth, processor failure, and high failure rates.
18. Testing & validation: Run unit tests: small file flows, chunking logic, manifest contents. Run integration tests: end-to-end with sample 1M-row files; verify chunking, manifest, Airflow trigger and bulk job behavior 
19. Documentation & runbooks: Document process group logic, parameter contexts, expected behaviors, and SRE runbooks for failures (e.g., S3 permission error, Pulsar unreachable, Airflow 401).
20. Import practice: Import into a staging NiFi instance first. Replace placeholder controller service names and test processors with small sample files. Only then promote to production cluster. 

Out of scope: 
1. Integrating the NiFi Registry promotion process or the Promotion using NiFi REST API using a CI/CD pipeline.
2. Custom NiFi processors TestRunner, TestRunnerFactory using org.apache.nifi.util.TestRunner for automated unit testing of the flows.

Correlation_id and manifest_path at the file (FlowFile) - Traceability for messages:

    NiFi should add correlation_id and manifest_path at the file (FlowFile) level and propagate them into downstream records as attributes — don’t split into one-FlowFile-per-record unless you absolutely must. The heavy cost is splitting into many FlowFiles (provenance + content repo + scheduling overhead), not the attribute itself.

Why, and what to do instead (actionable bullets)

Attributes vs FlowFiles:
1. Setting attributes on a FlowFile (e.g. correlation_id, manifest_path) is very cheap. NiFi copies parent attributes to child FlowFiles created by SplitRecord / PartitionRecord automatically.
2. The expensive operation is creating lots of FlowFiles (e.g., 1M FlowFiles for 1M rows): each FlowFile creates provenance events, scheduling overhead, content repo I/O and GC. Avoid that.

Recommended pattern (fast & safe)
1. Generate correlation_id and manifest_path once per file (before any split) with UpdateAttribute / EvaluateJsonPath.
2. Use chunking (PartitionRecord or SplitRecord with a chunk size) — split large files into chunks (e.g., 5k–50k rows per chunk), not single-record FlowFiles. Each chunk FlowFile carries the file-level attributes.
3. Publish records in batches rather than one-by-one: use PublishPulsarRecord (or PublishKafkaRecord) with a Record Writer that emits a RecordSet of many rows, or configure the processor to publish multiple records per FlowFile. This drastically reduces FlowFile count and overhead.
3. Enable producer-side batching (Pulsar producer batching / Kafka batching) — set batch.size / batch publish options so the broker sees fewer larger messages.
4. If you must produce single-record messages for downstream consumers, do the chunk->publish approach where the chunking processor emits many messages in one operation (RecordSet) but the producer still writes each record — this is still more efficient than 1M FlowFiles.

Recommended NiFi settings:

1. SplitRecord / PartitionRecord: set Record Count = 5000–50,000 depending on downstream DB throughput (start 50k, tune down on contention).
2. PublishPulsarRecord: enable batching; set batch size ~100 messages and enable asynchronous sends. Set Concurrent Tasks to 4–8.
3. ListS3 polling / FetchS3Object concurrency: moderate (1–4) to avoid huge bursts of chunk processing simultaneously.
4. Back-pressure: set connection thresholds to e.g. 5,000 FlowFiles OR 512 MB (so chunk queues don’t grow unbounded).
5. Provenance: reduce retention for extremely high-volume flows if regulatory allows; or archive only chunk manifests instead of entire provenance for every record.

Alternatives if downstream requires per-record FlowFiles

1. Offload heavy enrichment to Flink (keep NiFi lightweight). Current requirement does not include enrichment. NiFi just publishes raw records with minimal attributes; Flink does heavy per-record work.
2. Or use a hybrid: NiFi publishes per-record messages to Pulsar but doesn’t create 1M FlowFiles — instead publish using PublishPulsarRecord from a RecordSet, which serializes and publishes each record but keeps FlowFile count low.

Example flow (fast): ListS3 -> FetchS3Object -> UpdateAttribute (set correlation_id, manifest_path) -> SplitRecord (Record Count = 50k) -> ConvertRecord/PublishPulsarRecord (RecordSet writer with batching) -> PutS3Object archive (optional).

Quantify the penalty (rule-of-thumb) :Creating thousands of FlowFiles is OK; millions is expensive. Aim to keep FlowFiles ≤ a few thousand concurrently. Chunking + batching reduces scheduler and provenance pressure by orders of magnitude.

In the Small Flow, the Batch Sizes are to be kept up to 5000, and in Bulk Stager, up to 50000.