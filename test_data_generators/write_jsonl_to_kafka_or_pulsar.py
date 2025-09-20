#!/usr/bin/env python3
"""
jsonl_to_kafka_or_pulsar_parallel_progress_split.py

Produce a JSONL file (one JSON trade per line) to Kafka **or** Pulsar in parallel.

Two modes:
 - queue (default): master reads file and enqueues lines onto a bounded multiprocessing.Queue;
   workers consume the queue and produce to the selected messaging system. Shows queued / produced metrics.
 - split: file-splitting mode — each worker reads a separate byte-range (slice) of the file
   and produces directly. This avoids the central queue and can be more efficient for large files.

Usage examples:
  # Kafka (default)
  python jsonl_to_kafka_or_pulsar_parallel_progress_split.py --file trades.jsonl --topic trades-topic \
    --backend kafka --bootstrap-servers broker:9092 --workers 4 --mode queue --idempotent

  # Pulsar
  python jsonl_to_kafka_or_pulsar_parallel_progress_split.py --file trades.jsonl --topic trades-topic \
    --backend pulsar --pulsar-service-url pulsar://localhost:6650 --workers 4 --mode split

Notes:
 - For split mode the script computes byte ranges; it ensures workers start at a full line boundary.
 - Progress will show queued_count only in queue mode. In split mode queued_count will be 0 and produced_count
   will increase as workers produce messages.
 - Transactions: transactional support is implemented for Kafka (confluent_kafka). Pulsar transactions
   are not implemented in this script (pulsar-python client transaction APIs are more involved and
   require broker support and different usage patterns).

Dependencies:
 - confluent-kafka (for Kafka backend)
 - pulsar-client (for Pulsar backend) — install with `pip install pulsar-client` (package name may vary by platform)

"""

from __future__ import annotations
import argparse
import time
import sys
import multiprocessing as mp
import os
from typing import Optional

# JSON lib: prefer orjson if available for speed
try:
    import orjson as jsonlib

    def loads(s):
        return jsonlib.loads(s)
    def dumps(obj):
        return jsonlib.dumps(obj)
except Exception:
    import json as jsonlib

    def loads(s):
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        return jsonlib.loads(s)
    def dumps(obj):
        return jsonlib.dumps(obj, ensure_ascii=False).encode("utf-8")

# Kafka client
try:
    from confluent_kafka import Producer as CKProducer, KafkaException
except Exception as e:
    CKProducer = None

# Pulsar client (optional)
try:
    import pulsar
except Exception:
    pulsar = None


class AbstractProducerWrapper:
    """Abstract interface implemented by backend-specific wrappers."""
    def produce(self, key: Optional[bytes], value: bytes):
        raise NotImplementedError

    def poll(self, timeout: float = 0):
        """Optional: poll background events for the underlying client."""
        return None

    def flush(self, timeout: Optional[float] = None):
        raise NotImplementedError

    def init_transactions(self, timeout: float = 60.0):
        raise NotImplementedError

    def begin_transaction(self):
        raise NotImplementedError

    def commit_transaction(self):
        raise NotImplementedError

    def abort_transaction(self):
        raise NotImplementedError


class KafkaProducerWrapper(AbstractProducerWrapper):
    def __init__(self, bootstrap_servers: str, enable_idempotence: bool = True,
                 linger_ms: int = 5, batch_num_messages: int = 1000,
                 compression_type: str = "lz4", transactional_id: Optional[str] = None,
                 queue_buffering_max_kbytes: int = 1048576):
        if CKProducer is None:
            raise RuntimeError("confluent_kafka not available; install confluent-kafka")
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": linger_ms,
            "batch.num.messages": batch_num_messages,
            "compression.type": compression_type,
            "queue.buffering.max.kbytes": queue_buffering_max_kbytes,
        }
        if enable_idempotence:
            conf["enable.idempotence"] = True
            # keep max.in.flight reasonably high for throughput while still idempotent
            conf["max.in.flight.requests.per.connection"] = 5
        if transactional_id:
            conf["transactional.id"] = transactional_id
        self._producer = CKProducer(conf)
        self._transactional = transactional_id is not None

    def produce(self, key: Optional[bytes], value: bytes):
        # confluent-kafka accepts bytes for key/value
        try:
            self._producer.produce(key=key, value=value, callback=delivery_report)
        except BufferError:
            # caller should handle polling/reties
            raise

    def poll(self, timeout: float = 0):
        self._producer.poll(timeout)

    def flush(self, timeout: Optional[float] = None):
        # confluent_kafka flush returns number of messages still in queue; we ignore it here
        if timeout is None:
            self._producer.flush()
        else:
            self._producer.flush(timeout)

    def init_transactions(self, timeout: float = 60.0):
        if self._transactional:
            self._producer.init_transactions(timeout=timeout)

    def begin_transaction(self):
        if self._transactional:
            self._producer.begin_transaction()

    def commit_transaction(self):
        if self._transactional:
            self._producer.commit_transaction()

    def abort_transaction(self):
        if self._transactional:
            self._producer.abort_transaction()


class PulsarProducerWrapper(AbstractProducerWrapper):
    def __init__(self, service_url: str, topic: str,
                 batching_enabled: bool = True, batching_max_messages: int = 1000,
                 batching_max_publish_delay_ms: int = 10, producer_name: Optional[str] = None):
        if pulsar is None:
            raise RuntimeError("pulsar client not available; install pulsar-client")
        self._client = pulsar.Client(service_url)
        # create a producer for the topic
        self._producer = self._client.create_producer(
            topic,
            batching_enabled=batching_enabled,
            batching_max_messages=batching_max_messages,
            batching_max_publish_delay_ms= batching_max_publish_delay_ms,
            producer_name=producer_name,
        )
        # Pulsar Python client doesn't expose "transactions" in the same simple way as confluent_kafka.
        self._transactional = False

    def produce(self, key: Optional[bytes], value: bytes):
        # Pulsar expects bytes for the message; partition key is a string (not bytes)
        if key is not None:
            try:
                pk = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key)
            except Exception:
                pk = str(key)
            # use synchronous send to keep logic simpler and portable
            self._producer.send(value, partition_key=pk)
        else:
            self._producer.send(value)

    def poll(self, timeout: float = 0):
        # no-op for pulsar wrapper
        return None

    def flush(self, timeout: Optional[float] = None):
        # pulsar producer has flush
        try:
            if timeout is None:
                self._producer.flush()
            else:
                # pulsar-python flush doesn't accept timeout in older versions; ignore timeout
                self._producer.flush()
        except Exception:
            pass

    def init_transactions(self, timeout: float = 60.0):
        # Not implemented for Pulsar in this script
        pass

    def begin_transaction(self):
        pass

    def commit_transaction(self):
        pass

    def abort_transaction(self):
        pass


# delivery report for Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY-ERROR] {err} for message to {msg.topic()} partition {msg.partition()}", file=sys.stderr)


def create_producer(backend: str,
                    topic: str,
                    bootstrap_servers: str = "localhost:9092",
                    enable_idempotence: bool = True,
                    linger_ms: int = 5,
                    batch_num_messages: int = 1000,
                    compression_type: str = "lz4",
                    transactional_id: Optional[str] = None,
                    queue_buffering_max_kbytes: int = 1048576,
                    pulsar_service_url: Optional[str] = None,
                    pulsar_batching_max_publish_delay_ms: int = 10,
                    pulsar_producer_name: Optional[str] = None):
    """Factory that returns a backend-specific wrapper implementing AbstractProducerWrapper.

    For Kafka, bootstrap_servers and transactional_id are used.
    For Pulsar, pulsar_service_url must be provided.
    """
    backend = backend.lower()
    if backend == "kafka":
        return KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=enable_idempotence,
            linger_ms=linger_ms,
            batch_num_messages=batch_num_messages,
            compression_type=compression_type,
            transactional_id=transactional_id,
            queue_buffering_max_kbytes=queue_buffering_max_kbytes,
        )
    elif backend == "pulsar":
        if pulsar_service_url is None:
            raise RuntimeError("pulsar backend selected but --pulsar-service-url was not provided")
        return PulsarProducerWrapper(
            service_url=pulsar_service_url,
            topic=topic,
            batching_enabled=True,
            batching_max_messages=batch_num_messages,
            batching_max_publish_delay_ms=pulsar_batching_max_publish_delay_ms,
            producer_name=pulsar_producer_name,
        )
    else:
        raise ValueError(f"unknown backend: {backend}")


# worker functions updated to accept backend + pulsar args

def worker_from_queue(worker_id: int,
                      q: "mp.Queue",
                      produced_counter: "mp.Value",
                      produced_per_worker: "mp.Array",
                      topic: str,
                      backend: str,
                      bootstrap_servers: str,
                      key_field: str,
                      enable_idempotence: bool,
                      linger_ms: int,
                      batch_num_messages: int,
                      compression_type: str,
                      transactional_id_prefix: Optional[str],
                      flush_interval_sec: float,
                      shutdown_sentinel: bytes,
                      pulsar_service_url: Optional[str] = None):
    """Worker that consumes from a multiprocessing.Queue (queue mode)."""
    t_id = None
    if transactional_id_prefix and backend == "kafka":
        t_id = f"{transactional_id_prefix}-{worker_id}"

    producer = create_producer(
        backend=backend,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=enable_idempotence or (t_id is not None),
        linger_ms=linger_ms,
        batch_num_messages=batch_num_messages,
        compression_type=compression_type,
        transactional_id=t_id,
        pulsar_service_url=pulsar_service_url,
    )

    transactional = (t_id is not None) and backend == "kafka"
    if transactional:
        try:
            producer.init_transactions(timeout=60.0)
            producer.begin_transaction()
            print(f"[worker-{worker_id}] transactions initialized with id {t_id}")
        except Exception as e:
            print(f"[worker-{worker_id}] transaction init failed: {e}", file=sys.stderr)
            transactional = False

    produced_since_flush = 0
    last_flush = time.time()

    while True:
        try:
            raw = q.get()
        except Exception as e:
            print(f"[worker-{worker_id}] queue.get failed: {e}", file=sys.stderr)
            break

        if raw is None or (isinstance(raw, bytes) and raw == shutdown_sentinel):
            break

        try:
            rec = loads(raw)
        except Exception as e:
            print(f"[worker-{worker_id}] JSON parse error: {e}", file=sys.stderr)
            continue

        key = None
        if isinstance(rec, dict) and key_field in rec:
            key = str(rec[key_field]).encode("utf-8")
        try:
            val = dumps(rec)
        except Exception as e:
            print(f"[worker-{worker_id}] dumps error: {e}", file=sys.stderr)
            continue

        # produce (wrap backend-specific produce/poll semantics)
        produced_ok = False
        while not produced_ok:
            try:
                producer.produce(key=key, value=val)
                produced_ok = True
            except BufferError:
                # For Kafka this means local queue full; call poll and retry
                try:
                    producer.poll(0.1)
                except Exception:
                    time.sleep(0.1)
            except Exception as e:
                # Pulsar synchronous send may raise exceptions on network issues; retry a few times
                print(f"[worker-{worker_id}] produce error: {e}", file=sys.stderr)
                time.sleep(0.1)

        # increment produced counters atomically
        with produced_counter.get_lock():
            produced_counter.value += 1
        with produced_per_worker.get_lock():
            produced_per_worker[worker_id] = produced_per_worker[worker_id] + 1

        produced_since_flush += 1

        # allow background processing
        if produced_since_flush % 500 == 0:
            try:
                producer.poll(0)
            except Exception:
                pass

        now = time.time()
        if now - last_flush >= flush_interval_sec:
            try:
                if transactional:
                    producer.flush()
                    producer.commit_transaction()
                    producer.begin_transaction()
                else:
                    producer.flush()
            except Exception as ke:
                print(f"[worker-{worker_id}] flush/commit failed: {ke}", file=sys.stderr)
                if transactional:
                    try:
                        producer.abort_transaction()
                    except Exception as e:
                        print(f"[worker-{worker_id}] abort failed: {e}", file=sys.stderr)
                    try:
                        producer.begin_transaction()
                    except Exception as e:
                        print(f"[worker-{worker_id}] begin failed: {e}", file=sys.stderr)
            last_flush = now
            produced_since_flush = 0

    # final flush / commit
    try:
        if transactional:
            producer.flush()
            try:
                producer.commit_transaction()
            except Exception as e:
                print(f"[worker-{worker_id}] final commit failed: {e}", file=sys.stderr)
                try:
                    producer.abort_transaction()
                except Exception:
                    pass
        else:
            producer.flush()
    except Exception as e:
        print(f"[worker-{worker_id}] final flush error: {e}", file=sys.stderr)

    print(f"[worker-{worker_id}] exiting.")


def worker_file_slice(worker_id: int,
                      file_path: str,
                      start_byte: int,
                      end_byte: int,
                      produced_counter: "mp.Value",
                      produced_per_worker: "mp.Array",
                      topic: str,
                      backend: str,
                      bootstrap_servers: str,
                      key_field: str,
                      enable_idempotence: bool,
                      linger_ms: int,
                      batch_num_messages: int,
                      compression_type: str,
                      transactional_id_prefix: Optional[str],
                      flush_interval_sec: float,
                      pulsar_service_url: Optional[str] = None):
    """
    Worker that reads a byte-range slice from file_path and produces lines found within that range.
    start_byte inclusive, end_byte exclusive.
    """
    t_id = None
    if transactional_id_prefix and backend == "kafka":
        t_id = f"{transactional_id_prefix}-{worker_id}"

    producer = create_producer(
        backend=backend,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=enable_idempotence or (t_id is not None),
        linger_ms=linger_ms,
        batch_num_messages=batch_num_messages,
        compression_type=compression_type,
        transactional_id=t_id,
        pulsar_service_url=pulsar_service_url,
    )

    transactional = (t_id is not None) and backend == "kafka"
    if transactional:
        try:
            producer.init_transactions(timeout=60.0)
            producer.begin_transaction()
            print(f"[slice-worker-{worker_id}] transactions initialized with id {t_id}")
        except Exception as e:
            print(f"[slice-worker-{worker_id}] transaction init failed: {e}", file=sys.stderr)
            transactional = False

    produced_since_flush = 0
    last_flush = time.time()

    with open(file_path, "rb") as fh:
        # Move to start_byte; if not zero, skip partial line to start at the next newline char
        fh.seek(start_byte)
        if start_byte != 0:
            fh.readline()  # discard partial line

        while fh.tell() < end_byte:
            raw = fh.readline()
            if not raw:
                break
            pos_after = fh.tell()
            if pos_after > end_byte:
                # include full line (simple policy)
                pass

            if not raw.strip():
                continue

            try:
                rec = loads(raw)
            except Exception as e:
                print(f"[slice-worker-{worker_id}] JSON parse error: {e}", file=sys.stderr)
                continue

            key = None
            if isinstance(rec, dict) and key_field in rec:
                key = str(rec[key_field]).encode("utf-8")
            try:
                val = dumps(rec)
            except Exception as e:
                print(f"[slice-worker-{worker_id}] dumps error: {e}", file=sys.stderr)
                continue

            produced_ok = False
            while not produced_ok:
                try:
                    producer.produce(key=key, value=val)
                    produced_ok = True
                except BufferError:
                    try:
                        producer.poll(0.1)
                    except Exception:
                        time.sleep(0.1)
                except Exception as e:
                    print(f"[slice-worker-{worker_id}] produce error: {e}", file=sys.stderr)
                    time.sleep(0.1)

            # increment counters
            with produced_counter.get_lock():
                produced_counter.value += 1
            with produced_per_worker.get_lock():
                produced_per_worker[worker_id] = produced_per_worker[worker_id] + 1

            produced_since_flush += 1

            if produced_since_flush % 500 == 0:
                try:
                    producer.poll(0)
                except Exception:
                    pass

            now = time.time()
            if now - last_flush >= flush_interval_sec:
                try:
                    if transactional:
                        producer.flush()
                        producer.commit_transaction()
                        producer.begin_transaction()
                    else:
                        producer.flush()
                except Exception as ke:
                    print(f"[slice-worker-{worker_id}] flush/commit failed: {ke}", file=sys.stderr)
                    if transactional:
                        try:
                            producer.abort_transaction()
                        except Exception as e:
                            print(f"[slice-worker-{worker_id}] abort failed: {e}", file=sys.stderr)
                        try:
                            producer.begin_transaction()
                        except Exception as e:
                            print(f"[slice-worker-{worker_id}] begin failed: {e}", file=sys.stderr)
                last_flush = now
                produced_since_flush = 0

    # final flush / commit
    try:
        if transactional:
            producer.flush()
            try:
                producer.commit_transaction()
            except Exception as e:
                print(f"[slice-worker-{worker_id}] final commit failed: {e}", file=sys.stderr)
                try:
                    producer.abort_transaction()
                except Exception:
                    pass
        else:
            producer.flush()
    except Exception as e:
        print(f"[slice-worker-{worker_id}] final flush error: {e}", file=sys.stderr)

    print(f"[slice-worker-{worker_id}] exiting.")


# rest of script (master, compute_file_slices, progress printing) remains mostly unchanged

def master_enqueue_file(file_path: str,
                        q: "mp.Queue",
                        queued_counter: "mp.Value",
                        num_workers: int,
                        queue_backpressure_sleep: float,
                        shutdown_sentinel: bytes,
                        master_done_flag: "mp.Value"):
    """Master reads file and enqueues lines onto queue; increments queued_counter."""
    total = 0
    with open(file_path, "rb") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            put_ok = False
            while not put_ok:
                try:
                    q.put(raw, block=True, timeout=1.0)
                    put_ok = True
                except Exception:
                    time.sleep(queue_backpressure_sleep)
            with queued_counter.get_lock():
                queued_counter.value += 1
            total += 1
            if total % 10000 == 0:
                print(f"[master] queued {total} lines")

    # send sentinel for each worker
    for _ in range(num_workers):
        q.put(shutdown_sentinel)
    with master_done_flag.get_lock():
        master_done_flag.value = 1
    print(f"[master] done reading file, total queued {total}")


def compute_file_slices(file_path: str, num_workers: int) -> list[tuple[int, int]]:
    """Return list of (start_byte, end_byte) tuples for each worker. end_byte is exclusive."""
    file_size = os.path.getsize(file_path)
    if num_workers <= 1:
        return [(0, file_size)]
    slice_size = file_size // num_workers
    slices = []
    for i in range(num_workers):
        start = i * slice_size
        end = (i + 1) * slice_size if i < num_workers - 1 else file_size
        slices.append((start, end))
    return slices


def parse_args():
    p = argparse.ArgumentParser(description="Produce JSONL file trades to Kafka or Pulsar in parallel (queue or split mode).")
    p.add_argument("--file", "-f", required=True, help="Input JSONL file (one JSON object per line).")
    p.add_argument("--topic", "-t", required=True, help="Topic to produce to (Kafka topic or Pulsar topic).")
    p.add_argument("--backend", "-B", choices=["kafka", "pulsar"], default="kafka",
                   help="Backend messaging system: kafka or pulsar (default: kafka)")
    p.add_argument("--bootstrap-servers", "-b", required=False,
                   default="localhost:9092", help="Kafka bootstrap.servers (comma-separated). Used only for Kafka.")
    p.add_argument("--pulsar-service-url", required=False, default="pulsar://localhost:6650",
                   help="Pulsar broker service URL (pulsar://...) — used only for Pulsar backend.")
    p.add_argument("--workers", "-w", type=int, default=4, help="Number of producer worker processes.")
    p.add_argument("--mode", choices=["queue", "split"], default="queue",
                   help="queue: master enqueues lines; split: each worker reads a file slice.")
    p.add_argument("--queue-size", type=int, default=5000, help="Max items in the inter-process queue (queue mode).")
    p.add_argument("--queue-backpressure-sleep", type=float, default=0.05,
                   help="Sleep (s) when queue is full before retrying to put (queue mode).")
    p.add_argument("--key-field", default="Trade Id", help="JSON field to use as message key.")
    p.add_argument("--linger-ms", type=int, default=5, help="linger.ms for each Kafka producer (ms).")
    p.add_argument("--batch-num-messages", type=int, default=1000, help="batch.num.messages for each producer.")
    p.add_argument("--compression", default="lz4", help="compression.type for Kafka producer.")
    p.add_argument("--idempotent", action="store_true", help="Enable idempotent producer on workers (Kafka only).")
    p.add_argument("--transactional-id-prefix", default=None,
                   help="If provided (Kafka only), enable transactions using this prefix and appending worker id.")
    p.add_argument("--flush-interval", type=float, default=5.0, help="Seconds between periodic flush/commit per worker.")
    p.add_argument("--progress-interval", type=float, default=2.0, help="Seconds between progress prints.")
    return p.parse_args()


def main():
    args = parse_args()

    num_workers = max(1, args.workers)

    # shared counters and arrays
    queued_counter = mp.Value('i', 0)  # used only in queue mode
    produced_counter = mp.Value('i', 0)
    produced_per_worker = mp.Array('i', [0] * num_workers)
    master_done_flag = mp.Value('i', 0)

    # sentinel for queue mode
    shutdown_sentinel = b"__JSONL_MSG_SHUTDOWN_SENTINEL__"

    workers = []

    if args.mode == "queue":
        # bounded queue
        q = mp.Queue(maxsize=max(10, args.queue_size))

        # start worker processes
        for i in range(num_workers):
            p = mp.Process(
                target=worker_from_queue,
                args=(
                    i, q, produced_counter, produced_per_worker,
                    args.topic, args.backend, args.bootstrap_servers, args.key_field,
                    args.idempotent, args.linger_ms, args.batch_num_messages,
                    args.compression, args.transactional_id_prefix,
                    args.flush_interval, shutdown_sentinel, args.pulsar_service_url
                ),
                daemon=False
            )
            p.start()
            workers.append(p)
            print(f"[main] started queue-worker-{i} (pid {p.pid})")

        # start master enqueueing in a separate process so main can monitor progress
        master_proc = mp.Process(
            target=master_enqueue_file,
            args=(args.file, q, queued_counter, num_workers, args.queue_backpressure_sleep, shutdown_sentinel, master_done_flag),
            daemon=False
        )
        master_proc.start()
        print(f"[main] started master enqueuer (pid {master_proc.pid})")

    else:  # split mode
        # compute byte slices
        slices = compute_file_slices(args.file, num_workers)
        for i, (start, end) in enumerate(slices):
            p = mp.Process(
                target=worker_file_slice,
                args=(
                    i, args.file, start, end, produced_counter, produced_per_worker,
                    args.topic, args.backend, args.bootstrap_servers, args.key_field,
                    args.idempotent, args.linger_ms, args.batch_num_messages,
                    args.compression, args.transactional_id_prefix, args.flush_interval, args.pulsar_service_url
                ),
                daemon=False
            )
            p.start()
            workers.append(p)
            print(f"[main] started slice-worker-{i} (pid {p.pid}) range=({start},{end})")
        # master is "done" immediately in split mode (no queue)
        with master_done_flag.get_lock():
            master_done_flag.value = 1

    # start progress printer in main thread (can also be a separate process)
    try:
        # run progress printing loop until workers exit or stable
        while True:
            queued = queued_counter.value if args.mode == "queue" else 0
            produced = produced_counter.value
            per_worker = [produced_per_worker[i] for i in range(num_workers)]
            print(f"[progress] queued={queued:,} produced={produced:,} per_worker={per_worker}")
            # termination conditions:
            #  - queue mode: when master_done and produced >= queued (and workers have likely flushed)
            #  - split mode: when all workers have exited
            if args.mode == "queue":
                if master_done_flag.value == 1 and produced >= queued:
                    # wait briefly to allow any in-flight flushes, then break
                    stable = True
                    for _ in range(3):
                        time.sleep(args.progress_interval)
                        new_produced = produced_counter.value
                        if new_produced != produced:
                            stable = False
                            produced = new_produced
                            break
                    if stable:
                        break
            else:  # split mode
                alive = any(p.is_alive() for p in workers)
                if not alive:
                    break
            time.sleep(args.progress_interval)
    except KeyboardInterrupt:
        print("[main] interrupted; attempting graceful shutdown...", file=sys.stderr)
        # queue mode: put sentinels
        if args.mode == "queue":
            for _ in range(num_workers):
                try:
                    q.put(shutdown_sentinel)
                except Exception:
                    pass

    # join processes
    if args.mode == "queue":
        # ensure master process finishes
        master_proc.join(timeout=60)
        for p in workers:
            p.join(timeout=300)
            if p.is_alive():
                print(f"[main] worker pid {p.pid} still alive; terminating...", file=sys.stderr)
                p.terminate()
                p.join()
    else:
        for p in workers:
            p.join(timeout=300)
            if p.is_alive():
                print(f"[main] worker pid {p.pid} still alive; terminating...", file=sys.stderr)
                p.terminate()
                p.join()

    print("[main] all done. Final counts:")
    print(f"  queued:   {queued_counter.value:,}")
    print(f"  produced: {produced_counter.value:,}")
    print(f"  per_worker: {[produced_per_worker[i] for i in range(num_workers)]}")


if __name__ == "__main__":
    # Recommended on some platforms to avoid issues with fork/spawn
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        # already set
        pass
    main()
