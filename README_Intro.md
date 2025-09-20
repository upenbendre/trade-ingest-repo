
The file design_docs/trade_ingestion.pdf describes the target state comprising of 
1. Ingestion Layer (Apache Nifi)
2. Unified Event Backbone (Apache Pulsar)
3. Batch Orchestration Layer (Apache Airflow)
4. Central Stream and Batch Processing (Apache Flink)
5. Trade Store Distributed database (No SQL distributed DB supporting Postgres wire / Standard distributed RDBMS)
6. Variety of trade sources e.g. Kafka, S3 files, staging DBs etc.

Assumptions / Disclaimers:
1. Requirement is for front/middle office system, and not for backoffice / backend accounting OLAP store.
2. The requirements doc is a precursor to larger, scalable system req with more complex future needs around transformation, complex event processing, multi-tenancy etc.
3. It is assumed that Trade auto-expiry will be implemented either as a database / column constraint, or start of trading batch in Airflow. Not in scope of this design.

Disclaimers:
1. Uses Open-source stack with each technology having capability overlaps with another e.g. Kafka vs Pulsar, NiFi vs Airflow. Options open to consolidate the stack into fewer components e.g. (Kafka / S3 -> Nifi -> Flink / Spark -> Postgres) OR (Kafka/S3 -> Flink/Airflow 3)
2. If this was to be backend flow for OLAP / Lakehouse processing, Apache Iceberg / Spark with OLAP options like Postgres Citus were available.
3. Each of these tech components have better options in public clouds like AWS/GCP - options open.
4. The '__wip_code' directory contains Airflow, NiFi and Flink stream code, aligned to the enterprise design, but is partial and only indicative of the full-fledged solution comprising of the Ingestion Layer, Event Backbone and Batch Orchestration
5. Rest of the code is a Docker based project covering a simpler flow of Events from Apache Pulsar to Apache Flink jobs. 
6. ci.yml covers build, integration tests and lint/scans as in line with the requirements. May need customization to deploy on another environment.
7. test_data_generators directory has random trade data generators which use Polars based dataframe generation to create large volume of jsonl records in seconds.
8. The docker set up includes Pulsar, Flink, Vault with dependencies. Postgres is assumed to be running on the localhost.
9. Following components are heavily vibe-coded:
    - write_jsonl_to_kafka_or_pulsar.py
    - split_jsonl_to_parts.py
10. NiFi components were built on a local NiFi console, and the exported XML was heavy edited. Not likely to work out of the box.