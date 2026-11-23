"""Pure functions with no Kafka/Iceberg dependencies, so they can be unit-tested.

The interesting idea in this project lives here: Kafka offsets are stored *inside*
the Iceberg snapshot summary, so "data landed" and "offsets advanced" are one
atomic commit. On restart we read offsets back from the table, not from Kafka.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

# Snapshot-summary key holding {"<topic>": {"<partition>": <next_offset>, ...}}
OFFSETS_KEY = "kafka.offsets"

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_ts")
EVENT_TYPES = {"page_view", "add_to_cart", "purchase", "refund"}


class InvalidEvent(ValueError):
    """Raised for messages that should go to the dead-letter topic."""


def parse_event(raw: bytes | None, partition: int, offset: int, ingested_at: datetime) -> dict[str, Any]:
    """Turn a raw Kafka value into a row for the Iceberg table, or raise InvalidEvent."""
    if raw is None:
        raise InvalidEvent("null message value")
    try:
        obj = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidEvent(f"not valid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise InvalidEvent("JSON value is not an object")

    missing = [f for f in REQUIRED_FIELDS if obj.get(f) is None]
    if missing:
        raise InvalidEvent(f"missing required fields: {missing}")

    if obj["event_type"] not in EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type: {obj['event_type']!r}")

    try:
        user_id = int(obj["user_id"])
    except (TypeError, ValueError) as e:
        raise InvalidEvent(f"user_id is not an integer: {obj['user_id']!r}") from e

    amount = obj.get("amount")
    if amount is not None:
        try:
            amount = float(amount)
        except (TypeError, ValueError) as e:
            raise InvalidEvent(f"amount is not numeric: {amount!r}") from e

    return {
        "event_id": str(obj["event_id"]),
        "user_id": user_id,
        "event_type": obj["event_type"],
        "amount": amount,
        "event_ts": parse_ts(obj["event_ts"]),
        "_kafka_partition": partition,
        "_kafka_offset": offset,
        "_ingested_at": ingested_at,
    }


def parse_ts(value: Any) -> datetime:
    """Accept ISO-8601 strings or epoch milliseconds; always return an aware UTC datetime."""
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as e:
        raise InvalidEvent(f"bad event_ts: {value!r}") from e
    raise InvalidEvent(f"bad event_ts type: {type(value).__name__}")


def decode_offsets(summary_value: str | None) -> dict[str, dict[int, int]]:
    """Parse the JSON stored in the snapshot summary. Partition keys become ints."""
    if not summary_value:
        return {}
    data = json.loads(summary_value)
    return {topic: {int(p): int(o) for p, o in parts.items()} for topic, parts in data.items()}


def encode_offsets(offsets: Mapping[str, Mapping[int, int]]) -> str:
    """Compact, deterministic JSON (sorted keys) for the snapshot summary."""
    return json.dumps(
        {t: {str(p): o for p, o in sorted(parts.items())} for t, parts in sorted(offsets.items())},
        separators=(",", ":"),
    )


def merge_offsets(
    committed: Mapping[str, Mapping[int, int]],
    batch: Mapping[str, Mapping[int, int]],
) -> dict[str, dict[int, int]]:
    """Combine the previously committed offsets with this batch's.

    Partitions not in this batch keep their old offset, so the summary always holds
    the full picture. Offsets never move backwards.
    """
    out: dict[str, dict[int, int]] = {t: dict(p) for t, p in committed.items()}
    for topic, parts in batch.items():
        tgt = out.setdefault(topic, {})
        for p, o in parts.items():
            tgt[p] = max(o, tgt.get(p, -1))
    return out


def latest_committed_offsets(snapshot_summaries: Iterable[Mapping[str, str] | None]) -> dict[str, dict[int, int]]:
    """Walk snapshots newest -> oldest and return the first one carrying offsets.

    Snapshots written by something else (compaction, a manual delete) won't have
    the key, so we skip past them instead of assuming the head snapshot is ours.
    """
    for summary in snapshot_summaries:
        if summary is not None and summary.get(OFFSETS_KEY):
            return decode_offsets(summary.get(OFFSETS_KEY))
    return {}
