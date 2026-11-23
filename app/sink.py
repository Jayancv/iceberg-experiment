"""Kafka -> Iceberg sink with exactly-once delivery into the table.

How it works
------------
1. Consume a micro-batch of messages (by row count or time, whichever comes first).
2. Valid messages become rows; invalid ones go to a dead-letter topic.
3. Append the rows to Iceberg in ONE commit whose snapshot summary also records
   the next Kafka offset for every partition (`kafka.offsets`).
4. On startup / partition assignment, read those offsets back from the table and
   seek to them. Kafka's own consumer-group offsets are updated afterwards for lag
   monitoring only; they are never used to decide where to resume.

Because data and offsets are committed atomically, a crash at any point either
loses the whole in-flight batch (it's re-read from Kafka) or keeps all of it
(and its offsets). There's no window where rows land but offsets don't.

Failure policy is "crash and restart": if an Iceberg commit fails for any reason,
including an ambiguous one where we can't tell whether it succeeded, the process
exits. On restart it re-derives its position from the table, which is always right.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from iceberg_setup import IDENTIFIER, ensure_bucket, ensure_table, get_catalog
from logic import OFFSETS_KEY, InvalidEvent, encode_offsets, latest_committed_offsets, merge_offsets, parse_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s sink: %(message)s")
log = logging.getLogger("sink")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "events.dlq")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "iceberg-sink")
BATCH_MAX_ROWS = int(os.environ.get("BATCH_MAX_ROWS", "5000"))
BATCH_MAX_SECONDS = float(os.environ.get("BATCH_MAX_SECONDS", "15"))


def snapshot_summaries(table: Table):
    """Yield snapshot summaries as plain dicts, newest first, following parent links."""
    snap = table.current_snapshot()
    while snap is not None:
        summary = snap.summary
        if summary is None:
            yield {}
        else:
            props = getattr(summary, "additional_properties", None)
            yield dict(props) if props is not None else dict(summary)
        snap = table.snapshot_by_id(snap.parent_snapshot_id) if snap.parent_snapshot_id else None


class IcebergSink:
    def __init__(self) -> None:
        ensure_bucket()
        self.table = ensure_table(get_catalog())
        self.arrow_schema = schema_to_pyarrow(self.table.schema())
        log.info("writing to %s at %s", IDENTIFIER, self.table.location())

        self.consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": GROUP_ID,
                "enable.auto.commit": False,  # Iceberg is the source of truth for offsets
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        self.dlq = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
        self.running = True
        self._reset_batch()

    # -- batch state -----------------------------------------------------------
    def _reset_batch(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.batch_offsets: dict[int, int] = {}  # partition -> next offset to read
        self.batch_dlq = 0
        self.batch_started: float | None = None

    def committed_offsets(self) -> dict[str, dict[int, int]]:
        self.table.refresh()
        return latest_committed_offsets(snapshot_summaries(self.table))

    # -- rebalance callbacks ---------------------------------------------------
    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        committed = self.committed_offsets().get(TOPIC, {})
        for tp in partitions:
            tp.offset = committed.get(tp.partition, OFFSET_BEGINNING)
        log.info("assigned %s", {tp.partition: tp.offset for tp in partitions})
        consumer.assign(partitions)

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Uncommitted rows are simply dropped: whoever gets these partitions next
        # resumes from the offsets in the table and re-reads them.
        if self.rows or self.batch_offsets:
            log.info("partitions revoked; discarding %d uncommitted rows", len(self.rows))
        self._reset_batch()

    # -- main loop -------------------------------------------------------------
    def handle(self, msg) -> None:
        partition, offset = msg.partition(), msg.offset()
        try:
            self.rows.append(parse_event(msg.value(), partition, offset, datetime.now(UTC)))
        except InvalidEvent as e:
            self.dlq.produce(
                DLQ_TOPIC,
                key=msg.key(),
                value=msg.value(),
                headers={
                    "error": str(e)[:500],
                    "source.topic": TOPIC,
                    "source.partition": str(partition),
                    "source.offset": str(offset),
                },
            )
            self.dlq.poll(0)
            self.batch_dlq += 1
        # Track offsets for every message, valid or not, so DLQ'd messages
        # aren't re-read after a restart.
        self.batch_offsets[partition] = offset + 1
        if self.batch_started is None:
            self.batch_started = time.monotonic()

    def should_flush(self) -> bool:
        if not self.rows:
            # Nothing to write. Offsets of DLQ-only messages ride along with the next commit.
            return False
        age = time.monotonic() - (self.batch_started or time.monotonic())
        return len(self.rows) >= BATCH_MAX_ROWS or age >= BATCH_MAX_SECONDS

    def flush(self) -> None:
        if not self.rows:
            return
        # DLQ writes must be durable before the table says we're past those offsets.
        remaining = self.dlq.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} DLQ messages not delivered; refusing to commit")

        offsets = merge_offsets(self.committed_offsets(), {TOPIC: self.batch_offsets})
        data = pa.Table.from_pylist(self.rows, schema=self.arrow_schema)

        t0 = time.monotonic()
        # Single atomic commit: data files + offsets in the snapshot summary.
        self.table.append(
            data,
            snapshot_properties={
                OFFSETS_KEY: encode_offsets(offsets),
                "kafka.batch-rows": str(len(self.rows)),
                "kafka.batch-dlq": str(self.batch_dlq),
            },
        )
        snap = self.table.current_snapshot()
        log.info(
            "committed %d rows (+%d to DLQ) in %.2fs -> snapshot %s, offsets %s",
            len(self.rows),
            self.batch_dlq,
            time.monotonic() - t0,
            snap.snapshot_id if snap else "?",
            offsets[TOPIC],
        )

        # Mirror into the consumer group so `kafka-consumer-groups.sh --describe`
        # shows lag. Best effort: losing this is harmless.
        try:
            self.consumer.commit(
                offsets=[TopicPartition(TOPIC, p, o) for p, o in self.batch_offsets.items()],
                asynchronous=True,
            )
        except KafkaException as e:
            log.warning("could not mirror offsets to Kafka group (harmless): %s", e)

        self._reset_batch()

    def run(self) -> None:
        self.consumer.subscribe([TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)
        try:
            while self.running:
                for msg in self.consumer.consume(num_messages=1000, timeout=1.0):
                    if msg.error():
                        if msg.error().fatal():
                            raise KafkaException(msg.error())
                        log.warning("consumer error: %s", msg.error())
                        continue
                    self.handle(msg)
                if self.should_flush():
                    self.flush()
            log.info("shutting down: flushing final batch")
            self.flush()
        finally:
            self.consumer.close()
            self.dlq.flush(10)

    def stop(self, *_):
        self.running = False


def main() -> int:
    sink = IcebergSink()
    signal.signal(signal.SIGTERM, sink.stop)
    signal.signal(signal.SIGINT, sink.stop)
    try:
        sink.run()
    except Exception:
        # Crash-only: let the container restart and resume from the table's offsets.
        log.exception("fatal error; exiting so we restart from committed offsets")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
