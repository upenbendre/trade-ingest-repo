import json
import logging
from datetime import datetime, date

from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import RuntimeContext, RichSinkFunction

import psycopg2
from psycopg2.extras import Json
from pulsar import Client as PulsarClient

# -----------------------
# CONFIG
# -----------------------
PULSAR_URL = "pulsar://pulsar-broker:6650"
PULSAR_TOPIC = "persistent://front-office/trades/trades.incoming.normal"
PULSAR_DLQ = "persistent://front-office/trades/ingest.dlq"
POSTGRES_CONN = dict(
    host="postgres-host",
    port=5432,
    dbname="trades_db",
    user="trades_user",
    password="supersecret"
)

UPSERT_SQL = """
INSERT INTO trades (trade_id, version, counter_party_id, book_id,
                    maturity_date, created_date, payload, expired_flag, last_updated)
VALUES (%s, %s, %s, %s, %s, %s, %s, false, now())
ON CONFLICT (trade_id, version) DO UPDATE
SET counter_party_id = excluded.counter_party_id,
    book_id = excluded.book_id,
    maturity_date = excluded.maturity_date,
    created_date = excluded.created_date,
    payload = excluded.payload,
    expired_flag = excluded.expired_flag,
    last_updated = now()
WHERE trades.expired_flag = false;
"""

# -----------------------
# PROCESS FUNCTION
# -----------------------
class TradeProcessor(KeyedProcessFunction):

    def open(self, ctx: RuntimeContext):
        self.latest_version_state = ctx.get_state(ValueStateDescriptor("latest_version", Types.INT()))

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            trade = json.loads(value)
            trade_id = trade["trade_id"]
            version = int(trade["version"])
            maturity = datetime.strptime(trade["maturity_date"], "%Y-%m-%d").date()

            today = date.today()
            latest_version = self.latest_version_state.value()

            # Apply rejection rules
            if latest_version is not None:
                if version < latest_version:
                    ctx.output("dlq", json.dumps({"reason": "lower_version", "trade": trade}))
                    return
                if version == latest_version and maturity < today:
                    ctx.output("dlq", json.dumps({"reason": "expired_same_version", "trade": trade}))
                    return

            # Passed rules → update state
            if latest_version is None or version >= latest_version:
                self.latest_version_state.update(version)

            # Forward to Postgres sink
            ctx.output("to_postgres", json.dumps(trade))

        except Exception as e:
            ctx.output("dlq", json.dumps({"reason": f"parse_error: {e}", "raw": value}))


# -----------------------
# SINKS
# -----------------------
class PostgresSink(RichSinkFunction):
    def open(self, ctx):
        self.conn = psycopg2.connect(**POSTGRES_CONN)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def invoke(self, value, ctx):
        rec = json.loads(value)
        params = (
            rec["trade_id"],
            int(rec["version"]),
            rec["counter_party_id"],
            rec["book_id"],
            rec["maturity_date"],
            rec["created_date"],
            Json(rec.get("payload", {})),
        )
        self.cur.execute(UPSERT_SQL, params)

    def close(self):
        self.cur.close()
        self.conn.close()


class PulsarDLQSink(RichSinkFunction):
    def open(self, ctx):
        self.client = PulsarClient(PULSAR_URL)
        self.producer = self.client.create_producer(PULSAR_DLQ)

    def invoke(self, value, ctx):
        self.producer.send(value.encode("utf-8"))

    def close(self):
        self.producer.close()
        self.client.close()


# -----------------------
# JOB
# -----------------------
def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000)

    # Replace with Pulsar source (Java connector recommended in prod)
    source = env.from_collection(
        [], type_info=Types.STRING()
    )

    processed = source.key_by(lambda x: json.loads(x).get("trade_id", "NA")) \
                      .process(TradeProcessor())

    to_pg = processed.get_side_output("to_postgres")
    dlq = processed.get_side_output("dlq")

    to_pg.add_sink(PostgresSink()).name("postgres-sink")
    dlq.add_sink(PulsarDLQSink()).name("dlq-sink")

    env.execute("Trade Ingestion Job")


if __name__ == "__main__":
    run()
    
