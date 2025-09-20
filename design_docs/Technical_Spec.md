Apache Pulsar topic  persistent://front-office/trades/trades.incoming.normal with 32 partitions. It will have JSON messages with trades in the following format

1. trade_id - text
2. version - integer
3. counter_party_id - text
4. book_id - text
5. maturity_date - date
6. created_date - date
7. payload - can be nested json with other fields

These are to be ingested into a Postgres table using the following rules. The Postgres table will have another column expired_flag (boolean)

Expired vs Active:
1. Only trades with expired_flag = false (active) are considered when checking rejection rules.
2. Expiration is not handled by Flink in real time. Instead, an overnight batch job flips expired_flag → true for matured trades before trading hours begin.

Rejection Rules:
1. Rule 1: If an incoming trade has trade_id with lower version than the latest active version, it’s rejected.
2. Rule 2: If an incoming trade has trade_id + version matching an active record but its maturity_date < today, it’s rejected.
All rejected trades → DLQ topic persistent://front-office/trades/ingest.dlq.

Ingestion Rules:
1. If trade_id + version exists and is active → replace it (upsert).
2. If new trade_id or new version → insert as new active record.
3. If maturity_date < today → still insert, but as active (expired_flag=false).
(Overnight batch will later flip expired_flag=true.)

Postgres Guardrails:
1. Flink does near-time stateful filtering to reduce load.
2. But Postgres enforces rules too via SQL UPSERT constraints (so late/delayed events can’t break consistency).

yFlink application which does the above by consuming from Pulsar and writing to Postgres. Writes rejected Pulsar records to a dead letter queue at front-office/trades/ingest.dlq 

Assumes that the trade store table is already created in Postgres. We expect Idempotence, exactly once processing, accuracy. No 2PC commit with a staging and final table of Postgres. Can perform some rejects among records received in Flink at near-time, but will still need the same rejection rules as guardrail in the SQL query. 

out of scope -> An overnight process to mark trades with maturity_date past today, as expired. This should run before the trading hours begin, and the Flink application is active.
This is to be written for a front-office or middle office app, and not backend reporting.

Key Points in this Implementation:
1. Keyed State per trade_id: remembers the latest version of an active trade.
2. Rejection Rules implemented before writing to Postgres.
3. Postgres UPSERT ensures DB-level guardrails (no lower versions overwriting).
4. DLQ gets full JSON with rejection reason.
5. Exactly Once: achieved at-least once + idempotent UPSERT. (True 2PC XA is explicitly out of scope per your spec).

This design fits a front-office/middle-office ingestion pipeline:
1. Near-real-time
2. Accurate (DB + Flink guardrails)
3. Idempotent
4. No 2PC complexity
5. Overnight expiry handled separately.

Also provide a DDL of the Postgres table with proper indexes to improve read, while not affecting writes.
e.g. An index on maturity_dates where expired_flag is False. No need to index the expired records.
e.g. Consider CREATE TABLE with HASH partitioning of the table based on trade_id

optional: provide separate CREATE TABLE DDL with all same indexes, but also RANGE PARTITION on maturity date, instead of HASH Partitioning on trade_id.
Also implement -> ALTER TABLE trades SET (fillfactor = 90); , to avoid page bloating due to lot of tables.

In the PyFlink application , connection password or other credentials to Pulsar and Postgres should not be hard-coded. Implement a  method that fetches them from Hashicorp Vault. 

Also assuming there will be 20000 messages per minute in the Pulsar topic, configure Flink accordingly. Provide a docker based implementation with Pulsar, Flink and Vault. Only assume Postgres is external. The Postgres database is called tradedb, and schema called tradeschema.

Also provide GitHub actions to perform security scan on the code using a simple tool, build docker image including the Flink job and DDL source code from the github branch main.