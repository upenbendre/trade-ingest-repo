#!/usr/bin/env python3
"""PyFlink job: Pulsar -> Flink (pre-filtering keyed state) -> Postgres (upsert)
Rejected messages -> Pulsar DLQ (full JSON with reason)

This job is designed to run in a Flink cluster with the Pulsar connector jars available
and Python dependencies installed in the worker environment.
"""

import os
import json
import datetime
from typing import Any, Dict

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, OutputTag
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, RichSinkFunction
from pyflink.common import Types
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, Schema

# Vault
import hvac

# JDBC / DB
import psycopg2
from psycopg2.extras import Json as PsycoJson

# Pulsar client for sink-side DLQ publishing
import pulsar


# ---------- Helpers ----------

def get_credentials_from_vault(secret_path: str, keys: list):
    """Try HashiCorp Vault (kv v2) first; if it fails, fallback to environment variables."""
    vault_addr = os.getenv('VAULT_ADDR', 'http://127.0.0.1:8200')
    token = os.getenv('VAULT_TOKEN')
    client = hvac.Client(url=vault_addr, token=token)
    try:
        kv_path = secret_path
        if kv_path.startswith('secret/data/'):
            kv_path = kv_path[len('secret/data/'):]
        read = client.secrets.kv.v2.read_secret_version(path=kv_path, mount_point='secret')
        data = read['data']['data']
        return {k: data.get(k) or os.getenv(k.upper()) for k in keys}
    except Exception as e:
        print(f"[WARN] Vault read failed ({e}), falling back to environment variables for keys: {keys}")
        return {k: os.getenv(k.upper()) for k in keys}


def iso_date_to_date(s: str):
    if not s:
        return None
    try:
        return datetime.date.fromisoformat(s if len(s) == 10 else s.split('T')[0])
    except Exception:
        return None


# ---------- Process function ----------
class TradePreFilterProcessFunction(KeyedProcessFunction):
    """Keyed by trade_id. Emits accepted records to main output and JSON-string rejects to side-output."""
    def __init__(self, reject_output_tag: OutputTag):
        self.reject_output_tag = reject_output_tag
        self.latest_state = None

    def open(self, runtime_context: RuntimeContext):
        desc = ValueStateDescriptor("latest_active_version", Types.INT())
        self.latest_state = runtime_context.get_state(desc)

    def process_element(self, value: Dict[str, Any], ctx, out):
        today = datetime.date.today()
        trade = value
        trade_id = trade.get('trade_id')
        version = int(trade.get('version') or 0)
        maturity_date = iso_date_to_date(trade.get('maturity_date'))

        state_version = self.latest_state.value()

        # Rule 1
        if state_version is not None and version < state_version:
            payload = {
                "trade": trade,
                "reason": "RULE_1_LOWER_VERSION_THAN_ACTIVE",
                "detected_latest_active_version": state_version,
                "detected_at": datetime.datetime.utcnow().isoformat()
            }
            ctx.output(self.reject_output_tag, json.dumps(payload))
            return

        # Rule 2
        if state_version is not None and version == state_version:
            if maturity_date is not None and maturity_date < today:
                payload = {
                    "trade": trade,
                    "reason": "RULE_2_MATURITY_BEFORE_TODAY_FOR_ACTIVE_VERSION",
                    "detected_at": datetime.datetime.utcnow().isoformat()
                }
                ctx.output(self.reject_output_tag, json.dumps(payload))
                return

        # Accept
        if state_version is None or version >= (state_version or 0):
            self.latest_state.update(version)
        out.collect(trade)


# ---------- Postgres Upsert Sink (with DLQ publisher on DB-reject) ----------
class PostgresUpsertSink(RichSinkFunction):
    def __init__(self, pg_config: dict, pulsar_service_url: str, dlq_topic: str):
        super().__init__()
        self.pg_config = pg_config
        self.pulsar_service_url = pulsar_service_url
        self.dlq_topic = dlq_topic
        self.conn = None
        self.cur = None
        self.pulsar_client = None
        self.pulsar_producer = None

        self.upsert_sql = """
        INSERT INTO tradeschema.trades
            (trade_id, version, counter_party_id, book_id, maturity_date, created_date, payload, expired_flag, ingest_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (trade_id, version) DO UPDATE
          SET counter_party_id = EXCLUDED.counter_party_id,
              book_id = EXCLUDED.book_id,
              maturity_date = EXCLUDED.maturity_date,
              created_date = EXCLUDED.created_date,
              payload = EXCLUDED.payload,
              expired_flag = EXCLUDED.expired_flag,
              ingest_ts = now();
        """

    def open(self, runtime_context: RuntimeContext):
        self.conn = psycopg2.connect(
            dbname=self.pg_config.get('database'),
            user=self.pg_config.get('username'),
            password=self.pg_config.get('password'),
            host=self.pg_config.get('host'),
            port=self.pg_config.get('port', 5432),
            connect_timeout=10
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        try:
            self.pulsar_client = pulsar.Client(self.pulsar_service_url)
            self.pulsar_producer = self.pulsar_client.create_producer(self.dlq_topic)
        except Exception as e:
            print(f"[WARN] Could not create Pulsar client/producer for sink DLQ: {e}")
            self.pulsar_client = None
            self.pulsar_producer = None

    def invoke(self, value: Dict[str, Any], context):
        if value is None:
            return
        if value.get('_rejected'):
            return

        t = value
        try:
            payload_json = json.dumps(t.get('payload') or {})
            self.cur.execute(self.upsert_sql, (
                t.get('trade_id'),
                int(t.get('version') or 0),
                t.get('counter_party_id'),
                t.get('book_id'),
                t.get('maturity_date'),
                t.get('created_date'),
                PsycoJson(payload_json),
                False
            ))
        except Exception as e:
            msg = str(e)
            if "REJECTED_BY_DB_GUARDRAIL" in msg or "REJECTED_BY_DB" in msg or "REJECTED_BY_DB_GUARDRAIL" in msg.upper():
                dlq_payload = {
                    "trade": t,
                    "reason": "DB_GUARDRAIL_REJECT",
                    "db_error": msg,
                    "rejected_at": datetime.datetime.utcnow().isoformat()
                }
                published = False
                if self.pulsar_producer:
                    try:
                        self.pulsar_producer.send(json.dumps(dlq_payload).encode("utf-8"))
                        published = True
                    except Exception as pub_e:
                        print(f"[ERROR] Failed to publish DB guardrail reject to Pulsar DLQ: {pub_e}")
                        published = False
                if not published:
                    print("[ERROR] DLQ publish failed; record dropped (DB guardrail rejection):", json.dumps(dlq_payload))
                return
            else:
                raise

    def close(self):
        if self.cur:
            try:
                self.cur.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        if self.pulsar_producer:
            try:
                self.pulsar_producer.close()
            except Exception:
                pass
        if self.pulsar_client:
            try:
                self.pulsar_client.close()
            except Exception:
                pass


# ---------- MAIN ----------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "4")))
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    env.enable_checkpointing(10000)
    try:
        checkpoint_config = env.get_checkpoint_config()
        checkpoint_config.set_min_pause_between_checkpoints(5000)
        checkpoint_config.set_checkpoint_timeout(600000)
    except Exception:
        pass

    settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    creds = get_credentials_from_vault(secret_path='secret/data/trade/ingest', keys=[
        'pulsar_service_url', 'pulsar_admin_url', 'pulsar_dql_topic',
        'pg_host', 'pg_port', 'pg_database', 'pg_user', 'pg_password'
    ])

    pulsar_service_url = creds.get('pulsar_service_url') or os.getenv('PULSAR_SERVICE_URL', 'pulsar://pulsar:6650')
    pulsar_admin_url = creds.get('pulsar_admin_url') or os.getenv('PULSAR_ADMIN_URL', 'http://pulsar:8080')
    pulsar_topic = os.getenv('PULSAR_TOPIC', 'persistent://front-office/trades/trades.incoming.normal')
    pulsar_dlq_topic = creds.get('pulsar_dql_topic') or os.getenv('PULSAR_DLQ_TOPIC', 'persistent://front-office/trades/ingest.dlq')

    pg_config = {
        'host': creds.get('pg_host') or os.getenv('PG_HOST', 'host.docker.internal'),
        'port': int(creds.get('pg_port') or os.getenv('PG_PORT', 5432)),
        'database': creds.get('pg_database') or os.getenv('PG_DATABASE', 'postgres'),
        'username': creds.get('pg_user') or os.getenv('PG_USER', 'postgres'),
        'password': creds.get('pg_password') or os.getenv('PG_PASSWORD', 'postgres')
    }

    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS pulsar_trades (
          trade_id STRING,
          version INT,
          counter_party_id STRING,
          book_id STRING,
          maturity_date STRING,
          created_date STRING,
          payload STRING
        ) WITH (
          'connector' = 'pulsar',
          'topic' = '{pulsar_topic}',
          'service-url' = '{pulsar_service_url}',
          'admin-url' = '{pulsar_admin_url}',
          'format' = 'json',
          'scan.startup.mode' = 'latest'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS pulsar_dlq (
          value STRING
        ) WITH (
          'connector' = 'pulsar',
          'topic' = '{pulsar_dlq_topic}',
          'service-url' = '{pulsar_service_url}',
          'admin-url' = '{pulsar_admin_url}',
          'format' = 'raw'
        )
    """)

    trades_table = t_env.from_path('pulsar_trades')
    ds = t_env.to_append_stream(trades_table, Types.ROW([
        Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()
    ]))

    def row_to_dict(row):
        return {
            'trade_id': row[0],
            'version': row[1],
            'counter_party_id': row[2],
            'book_id': row[3],
            'maturity_date': row[4],
            'created_date': row[5],
            'payload': json.loads(row[6]) if row[6] else {}
        }

    mapped = ds.map(lambda r: row_to_dict(r), output_type=Types.PICKLED_BYTE_ARRAY())

    keyed = mapped.key_by(lambda v: v.get('trade_id', ''), key_type=Types.STRING())
    reject_output_tag = OutputTag("rejects", Types.STRING())
    processed = keyed.process(TradePreFilterProcessFunction(reject_output_tag), output_type=Types.PICKLED_BYTE_ARRAY())

    prefilter_rejects = processed.get_side_output(reject_output_tag)
    if prefilter_rejects is not None:
        dlq_table = t_env.from_data_stream(prefilter_rejects.map(lambda s: (s,), output_type=Types.ROW([Types.STRING()])), Schema.new_builder().column("value", DataTypes.STRING()).build())
        dlq_table.execute_insert("pulsar_dlq", overwrite=False)

    postgres_sink = PostgresUpsertSink(pg_config, pulsar_service_url, pulsar_dlq_topic)
    processed.add_sink(postgres_sink).set_parallelism(int(os.getenv("SINK_PARALLELISM", "4")))

    env.execute("trade-ingest-job")


if __name__ == "__main__":
    main()
