# trade_ingest_job.py
import os
import json
import datetime
from typing import Dict, Any

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, RichSinkFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.descriptors import Schema, Json, Kafka, ConnectorDescriptor

# For Vault
import hvac

# For JDBC
import psycopg2
from psycopg2.extras import Json as PsycoJson

# ---------- Vault credential helper ----------
def get_credentials_from_vault(secret_path: str, keys: list, vault_addr=None, role_id_env='VAULT_ROLE_ID', secret_id_env='VAULT_SECRET_ID'):
    """
    Fetch secrets from HashiCorp Vault using AppRole (role_id+secret_id) or fallback to environment variables.
    - secret_path: path in Vault to read (e.g., secret/data/trade/ingest)
    - keys: list of keys to extract
    Returns dict with requested keys or raises.
    """
    vault_addr = vault_addr or os.getenv('VAULT_ADDR', 'http://127.0.0.1:8200')
    client = hvac.Client(url=vault_addr)

    role_id = os.getenv(role_id_env)
    secret_id = os.getenv(secret_id_env)
    token = None

    try:
        if role_id and secret_id:
            auth = client.auth.approle.login(role_id=role_id, secret_id=secret_id)
            token = auth.get('auth', {}).get('client_token')
            client.token = token
        else:
            # fallback to environment token
            token_env = os.getenv('VAULT_TOKEN')
            if token_env:
                client.token = token_env

        # Adapt if using KV v2; hvac requires specifying mount and path appropriately.
        read = client.secrets.kv.v2.read_secret_version(path=secret_path.replace('secret/data/', ''), mount_point='secret')
        data = read['data']['data']
        return {k: data.get(k) or os.getenv(k.upper()) for k in keys}
    except Exception as e:
        # fallback to env vars for local dev
        print(f"Vault access failed: {e}. Falling back to env vars for keys {keys}.")
        return {k: os.getenv(k.upper()) for k in keys}


# ---------- Utilities ----------
def iso_date_to_date(s: str):
    # Accept 'YYYY-MM-DD' or ISO datetime strings
    if s is None:
        return None
    try:
        return datetime.date.fromisoformat(s if len(s) == 10 else s.split("T")[0])
    except Exception:
        return None

# ---------- Flink functions ----------
class TradePreFilterProcessFunction(KeyedProcessFunction):
    """
    Keyed by trade_id. State holds latest_active_version (int) for that trade_id.
    Applies the near-time rejection logic:
      - If incoming.version < latest_active_version => reject
      - If incoming.trade_id+version matches existing active version and maturity_date < today => reject
    Emits (accepted_flag, payload, reason) tuples by forwarding.
    """
    def open(self, runtime_context: RuntimeContext):
        latest_ver_desc = ValueStateDescriptor("latest_active_version", Types.INT())
        self.latest_state = runtime_context.get_state(latest_ver_desc)

    def process_element(self, value: Dict[str, Any], ctx: 'KeyedProcessFunction.Context'):
        # value is a dict representing the trade JSON
        today = datetime.date.today()
        trade_id = value.get('trade_id')
        version = int(value.get('version') or 0)
        maturity_date = iso_date_to_date(value.get('maturity_date') or value.get('maturityDate'))
        state_version = self.latest_state.value()
        # Pre-check
        if state_version is not None and version < state_version:
            # reject by reason Rule 1
            out = {
                "trade": value,
                "reason": "RULE_1_LOWER_VERSION_THAN_ACTIVE",
                "detected_latest_active_version": state_version,
                "detected_at": datetime.datetime.utcnow().isoformat()
            }
            # emit rejected flagged tuple
            self.output_tag = None
            # send to downstream via collect with special envelope
            self.collect_reject(ctx, out)
            return
        # If same trade_id + version equals state_version (active) and maturity_date < today -> reject (Rule 2)
        if state_version is not None and version == state_version:
            if maturity_date is not None and maturity_date < today:
                out = {
                    "trade": value,
                    "reason": "RULE_2_MATURITY_BEFORE_TODAY_FOR_ACTIVE_VERSION",
                    "detected_at": datetime.datetime.utcnow().isoformat()
                }
                self.collect_reject(ctx, out)
                return
        # Accept: update state if incoming version >= state_version
        if state_version is None or version >= (state_version or 0):
            # If this is active (we treat incoming as active despite maturity < today per your spec)
            self.latest_state.update(version)
        # For accepted, wrap and forward
        out_accept = {
            "trade": value,
            "accepted_at": datetime.datetime.utcnow().isoformat()
        }
        # Forward accepted
        ctx.output(None, out_accept)

    def collect_reject(self, ctx, payload):
        # Use ctx.output to emit to side output? In DataStream API we'd use side outputs.
        # But to keep code portable, we'll raise a special Exception/Flag pattern is not ideal.
        # We'll instead write rejected messages to a dedicated Pulsar producer in the sink path
        # For now just forward a dict with a field "rejected": True
        payload['_rejected'] = True
        ctx.output(None, payload)


class PostgresUpsertSink(RichSinkFunction):
    """
    Simple JDBC sink that performs an idempotent upsert into Postgres.
    On DB exception due to guardrail triggers, it will raise an exception which the job
    should catch upstream (or the function handles to publish to DLQ).
    """

    def __init__(self, pg_config):
        super().__init__()
        self.pg_config = pg_config
        self.conn = None
        self.cur = None

        # Prepared upsert statement uses ON CONFLICT (trade_id, version)
        # We update all non-key columns and set expired_flag = excluded.expired_flag
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
        # connect using provided config
        self.conn = psycopg2.connect(
            dbname=self.pg_config.get('database') or 'tradedb',
            user=self.pg_config.get('username'),
            password=self.pg_config.get('password'),
            host=self.pg_config.get('host'),
            port=self.pg_config.get('port') or 5432,
            connect_timeout=10
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def invoke(self, value, context):
        """
        value is an envelope:
        - if rejected: contains '_rejected' true -> let higher-level code handle DLQ
        - else: {'trade': {...}, 'accepted_at': ...}
        """
        # handle rejects outside this sink; assume only accepted messages reach here.
        if value.get('_rejected'):
            # Should not get here; if it happens, skip
            return

        t = value['trade']
        # Prepare fields
        trade_id = t.get('trade_id')
        version = int(t.get('version') or 0)
        counter_party_id = t.get('counter_party_id')
        book_id = t.get('book_id')
        maturity_date = t.get('maturity_date')
        created_date = t.get('created_date')
        payload = json.dumps(t.get('payload') or {})

        # execute upsert
        try:
            self.cur.execute(self.upsert_sql, (
                trade_id, version, counter_party_id, book_id,
                maturity_date, created_date, PsycoJson(payload),
                False  # incoming is inserted as active (expired_flag=false)
            ))
        except Exception as e:
            # On constraint/trig error, rethrow to let caller handle DLQ
            raise

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


# ---------- Main job ----------
def main():
    # env
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # adjust based on cluster; see recommendations below
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Fetch secrets (Pulsar & Postgres) from Vault or env
    creds = get_credentials_from_vault(secret_path='secret/data/trade/ingest', keys=[
        'pulsar_service_url', 'pulsar_admin_url', 'pulsar_tenant', 'pulsar_namespace',
        'pg_host', 'pg_port', 'pg_database', 'pg_user', 'pg_password',
        'pulsar_dql_topic'
    ])

    pulsar_service_url = creds.get('pulsar_service_url') or os.getenv('PULSAR_SERVICE_URL', 'pulsar://pulsar:6650')
    pulsar_admin_url = creds.get('pulsar_admin_url') or os.getenv('PULSAR_ADMIN_URL', 'http://pulsar:8080')
    pulsar_topic = 'persistent://front-office/trades/trades.incoming.normal'
    pulsar_dlq = creds.get('pulsar_dql_topic') or os.getenv('PULSAR_DLQ_TOPIC', 'persistent://front-office/trades/ingest.dlq')

    pg_config = {
        'host': creds.get('pg_host') or os.getenv('PG_HOST'),
        'port': creds.get('pg_port') or os.getenv('PG_PORT', 5432),
        'database': creds.get('pg_database') or os.getenv('PG_DATABASE', 'tradedb'),
        'username': creds.get('pg_user') or os.getenv('PG_USER'),
        'password': creds.get('pg_password') or os.getenv('PG_PASSWORD'),
    }

    # NOTE: The Pulsar connector must be available as a Flink connector jar in classpath,
    # e.g. flink-connector-pulsar-<version>.jar and its dependencies.
    #
    # We define a Pulsar Table source using SQL DDL (this depends on the connector provider)
    #
    # Alternatively you can use an external source to write to a Kafka-style connector or
    # implement a custom SourceFunction that pulls from pulsar-client. For production
    # prefer Flink's official Pulsar connector jar on the classpath.

    # Register a Pulsar source table (example using a generic connector syntax)
    # NOTE: adapt connector property names to the specific Pulsar connector you install.
    t_env.execute_sql(f"""
        CREATE TABLE pulsar_trades (
          trade_id STRING,
          version INT,
          counter_party_id STRING,
          book_id STRING,
          maturity_date STRING,
          created_date STRING,
          payload STRING
        ) WITH (
          'connector' = 'pulsar', -- requires Pulsar connector jar
          'topic' = '{pulsar_topic}',
          'service-url' = '{pulsar_service_url}',
          'admin-url' = '{pulsar_admin_url}',
          'format' = 'json',
          'scan.startup.mode' = 'latest'
        )
    """)

    # Also create a Pulsar sink table for DLQ (simple JSON)
    t_env.execute_sql(f"""
        CREATE TABLE pulsar_dlq (
          value STRING
        ) WITH (
          'connector' = 'pulsar',
          'topic' = '{pulsar_dlq}',
          'service-url' = '{pulsar_service_url}',
          'admin-url' = '{pulsar_admin_url}',
          'format' = 'raw' -- the connector should send raw bytes / JSON
        )
    """)

    # Read from Pulsar as a Table -> convert to DataStream of dicts.
    trades_tbl = t_env.from_path('pulsar_trades')
    ds = t_env.to_append_stream(trades_tbl, Types.ROW([
        Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING()
    ]))

    # Map to dict envelope
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

    # Assign timestamps/watermarks if event-time needed (optional)
    # For now we will use processing time and keyed state

    # Key by trade_id and apply prefilter process function
    keyed = mapped.key_by(lambda v: v.get('trade_id', ''), key_type=Types.STRING())
    processed = keyed.process(TradePreFilterProcessFunction(), output_type=Types.PICKLED_BYTE_ARRAY())

    # Split: rejected vs accepted - here we detect by presence of '_rejected' key.
    def route_to_sinks(x):
        if isinstance(x, dict) and x.get('_rejected'):
            # Write to DLQ topic directly using a simple producer approach:
            return ('dlq', json.dumps(x))
        else:
            return ('accept', x)

    routed = processed.map(lambda x: route_to_sinks(x), output_type=Types.PICKLED_BYTE_ARRAY())

    # Separate streams
    dlq_stream = routed.filter(lambda kv: kv[0] == 'dlq').map(lambda kv: kv[1], output_type=Types.STRING())
    accept_stream = routed.filter(lambda kv: kv[0] == 'accept').map(lambda kv: kv[1], output_type=Types.PICKLED_BYTE_ARRAY())

    # Write DLQ to Pulsar using the Pulsar sink table: convert to table & insert
    # Create a temporary in-memory Table from dlq_stream and insert to pulsar_dlq
    from pyflink.table import Table

    dlq_table = t_env.from_data_stream(dlq_stream, schema=Schema().field("value", DataTypes.STRING()))
    dlq_table.execute_insert("pulsar_dlq", overwrite=False)

    # Write accepts to Postgres sink via RichSinkFunction
    accept_stream.add_sink(PostgresUpsertSink(pg_config)).set_parallelism(4)

    env.execute("trade-ingest-job")


if __name__ == '__main__':
    main()
