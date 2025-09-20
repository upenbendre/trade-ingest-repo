Extract current business rules into a single spec (YAML/JSON).
Implement and publish a shared validation library (Python + optional Java wrapper).
Update streaming and batch jobs to call shared validator before attempting DB upsert; on reject, write to DLQ/rejection store with reason.
Implement conditional MERGE into trade_store as DB final guardrail.
Add unit/integration tests and CI gates for rule changes.
Add metrics and a daily reconciliation job.