# test_trade_job.py
import unittest
import json
import datetime
from unittest import mock

# adjust import if your file name differs
from trade_ingest_job import TradePreFilterProcessFunction, PostgresUpsertSink

# ---------- Fakes for ValueState / RuntimeContext / Collector / Ctx ----------

class FakeValueState:
    def __init__(self, initial=None):
        self._v = initial
    def value(self):
        return self._v
    def update(self, v):
        self._v = v

class FakeRuntimeContext:
    def __init__(self, state):
        self._state = state
    def get_state(self, desc):
        return self._state

class FakeCollector:
    def __init__(self):
        self.collected = []
    def collect(self, val):
        self.collected.append(val)

class FakeCtx:
    def __init__(self):
        self.side_outputs = []
    def output(self, output_tag, value):
        self.side_outputs.append((output_tag, value))


# ---------- Tests for TradePreFilterProcessFunction ----------

class TestTradePreFilterProcessFunction(unittest.TestCase):
    def setUp(self):
        self.reject_tag = "rejects"  # matches how tests check tag equality
        self.fn = TradePreFilterProcessFunction(reject_output_tag=self.reject_tag)
        self.fake_state = FakeValueState(initial=None)
        self.runtime_ctx = FakeRuntimeContext(self.fake_state)
        self.fn.open(self.runtime_ctx)

    def test_first_version_is_accepted_and_state_updated(self):
        collector = FakeCollector()
        ctx = FakeCtx()
        trade = {'trade_id': 'T1', 'version': 1, 'maturity_date': (datetime.date.today() + datetime.timedelta(days=10)).isoformat()}
        self.fn.process_element(trade, ctx, collector)
        self.assertEqual(len(collector.collected), 1)
        self.assertEqual(collector.collected[0]['trade_id'], 'T1')
        self.assertEqual(self.fake_state.value(), 1)
        self.assertEqual(len(ctx.side_outputs), 0)

    def test_reject_if_lower_version_than_state(self):
        self.fake_state.update(5)
        collector = FakeCollector()
        ctx = FakeCtx()
        trade = {'trade_id': 'T1', 'version': 3, 'maturity_date': (datetime.date.today()+datetime.timedelta(days=10)).isoformat()}
        self.fn.process_element(trade, ctx, collector)
        self.assertEqual(len(collector.collected), 0)
        self.assertEqual(len(ctx.side_outputs), 1)
        tag, payload_json = ctx.side_outputs[0]
        self.assertEqual(tag, self.reject_tag)
        payload = json.loads(payload_json)
        self.assertEqual(payload['reason'], 'RULE_1_LOWER_VERSION_THAN_ACTIVE')

    def test_reject_if_same_version_and_maturity_before_today(self):
        self.fake_state.update(2)
        collector = FakeCollector()
        ctx = FakeCtx()
        trade = {'trade_id': 'T1', 'version': 2, 'maturity_date': (datetime.date.today() - datetime.timedelta(days=1)).isoformat()}
        self.fn.process_element(trade, ctx, collector)
        self.assertEqual(len(collector.collected), 0)
        self.assertEqual(len(ctx.side_outputs), 1)
        payload = json.loads(ctx.side_outputs[0][1])
        self.assertEqual(payload['reason'], 'RULE_2_MATURITY_BEFORE_TODAY_FOR_ACTIVE_VERSION')

    def test_accept_if_same_version_and_maturity_today_or_future(self):
        self.fake_state.update(2)
        collector = FakeCollector()
        ctx = FakeCtx()
        trade = {'trade_id': 'T1', 'version': 2, 'maturity_date': datetime.date.today().isoformat()}
        self.fn.process_element(trade, ctx, collector)
        self.assertEqual(len(collector.collected), 1)
        self.assertEqual(self.fake_state.value(), 2)


# ---------- Fakes for PostgresUpsertSink tests ----------

class FakeCursor:
    def __init__(self, raise_on_execute=None):
        self.raise_on_execute = raise_on_execute
        self.executed = []
    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.raise_on_execute:
            raise self.raise_on_execute
    def close(self):
        pass

class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = False
    def cursor(self):
        return self._cursor
    def close(self):
        pass

class FakeProducer:
    def __init__(self):
        self.sent = []
    def send(self, payload):
        self.sent.append(payload)

class FakePulsarClient:
    def __init__(self, producer):
        self._producer = producer
    def create_producer(self, topic):
        return self._producer
    def close(self):
        pass


# ---------- Tests for PostgresUpsertSink ----------

class TestPostgresUpsertSink(unittest.TestCase):
    @mock.patch('trade_ingest_job.psycopg2.connect')
    @mock.patch('trade_ingest_job.pulsar.Client')
    def test_upsert_success_does_not_publish_dlq(self, mock_pulsar_client_cls, mock_psycopg_connect):
        fake_cursor = FakeCursor()
        fake_conn = FakeConnection(fake_cursor)
        mock_psycopg_connect.return_value = fake_conn

        fake_producer = FakeProducer()
        mock_pulsar_client_cls.return_value = FakePulsarClient(fake_producer)

        sink = PostgresUpsertSink(pg_config={'host':'x','username':'u','password':'p','database':'d','port':5432},
                                   pulsar_service_url='pulsar://x:6650',
                                   dlq_topic='persistent://ns/topic.dlq')
        sink.open(runtime_context=None)

        rec = {'trade_id':'T1','version':1,'counter_party_id':'CP','book_id':'B',
               'maturity_date':'2026-01-01','created_date':'2025-01-01','payload':{}, '_rejected': False}
        sink.invoke(rec, context=None)

        # should have executed one SQL
        self.assertEqual(len(fake_cursor.executed), 1)
        self.assertEqual(len(fake_producer.sent), 0)
        sink.close()

    @mock.patch('trade_ingest_job.psycopg2.connect')
    @mock.patch('trade_ingest_job.pulsar.Client')
    def test_db_guardrail_reject_publishes_dlq(self, mock_pulsar_client_cls, mock_psycopg_connect):
        err = Exception("REJECTED_BY_DB_GUARDRAIL: active higher version exists")
        fake_cursor = FakeCursor(raise_on_execute=err)
        fake_conn = FakeConnection(fake_cursor)
        mock_psycopg_connect.return_value = fake_conn

        fake_producer = FakeProducer()
        mock_pulsar_client_cls.return_value = FakePulsarClient(fake_producer)

        sink = PostgresUpsertSink(pg_config={'host':'x','username':'u','password':'p','database':'d','port':5432},
                                   pulsar_service_url='pulsar://x:6650',
                                   dlq_topic='persistent://ns/topic.dlq')
        sink.open(runtime_context=None)

        rec = {'trade_id':'T1','version':1,'counter_party_id':'CP','book_id':'B',
               'maturity_date':'2020-01-01','created_date':'2020-01-01','payload':{}, '_rejected': False}
        sink.invoke(rec, context=None)

        # producer should have at least one message (bytes payload)
        self.assertGreaterEqual(len(fake_producer.sent), 1)
        sent_payload = fake_producer.sent[0]
        try:
            decoded = json.loads(sent_payload.decode('utf-8') if isinstance(sent_payload, (bytes, bytearray)) else sent_payload)
        except Exception:
            self.fail("DLQ payload was not valid JSON")
        self.assertIn('reason', decoded)
        self.assertEqual(decoded['reason'], 'DB_GUARDRAIL_REJECT')

        sink.close()


if __name__ == '__main__':
    unittest.main()
