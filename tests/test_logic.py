import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from logic import (
    OFFSETS_KEY,
    InvalidEvent,
    decode_offsets,
    encode_offsets,
    latest_committed_offsets,
    merge_offsets,
    parse_event,
    parse_ts,
)

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def ev(**overrides):
    base = {
        "event_id": "e1",
        "user_id": 42,
        "event_type": "purchase",
        "amount": 19.99,
        "event_ts": "2026-11-20T10:00:00+00:00",
    }
    base.update(overrides)
    return json.dumps(base).encode()


# --- parse_event ---------------------------------------------------------------


def test_valid_event_becomes_row_with_lineage():
    row = parse_event(ev(), partition=2, offset=17, ingested_at=NOW)
    assert row["user_id"] == 42
    assert row["amount"] == 19.99
    assert row["event_ts"] == datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    assert (row["_kafka_partition"], row["_kafka_offset"], row["_ingested_at"]) == (2, 17, NOW)


def test_amount_is_optional():
    assert parse_event(ev(event_type="page_view", amount=None), 0, 0, NOW)["amount"] is None


def test_user_id_string_number_is_coerced():
    assert parse_event(ev(user_id="7"), 0, 0, NOW)["user_id"] == 7


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"{not json",
        b"\xff\xfe",
        b"[1, 2, 3]",
        ev(event_type="teleport"),
        ev(user_id="abc"),
        ev(amount="lots"),
        ev(event_ts="yesterday"),
        json.dumps({"event_id": "x", "event_type": "page_view"}).encode(),
    ],
)
def test_invalid_messages_raise(raw):
    with pytest.raises(InvalidEvent):
        parse_event(raw, 0, 0, NOW)


# --- parse_ts ------------------------------------------------------------------


def test_ts_z_suffix_and_naive_are_utc():
    assert parse_ts("2026-11-20T10:00:00Z") == datetime(2026, 11, 20, 10, tzinfo=UTC)
    assert parse_ts("2026-11-20T10:00:00") == datetime(2026, 11, 20, 10, tzinfo=UTC)


def test_ts_offset_is_normalised_to_utc():
    assert parse_ts("2026-11-20T12:00:00+02:00") == datetime(2026, 11, 20, 10, tzinfo=UTC)


def test_ts_epoch_millis():
    assert parse_ts(1_700_000_000_000) == datetime.fromtimestamp(1_700_000_000, tz=UTC)


def test_ts_bool_rejected():
    with pytest.raises(InvalidEvent):
        parse_ts(True)


# --- offsets -------------------------------------------------------------------


def test_offsets_roundtrip_and_are_deterministic():
    offsets = {"events": {2: 30, 0: 10, 1: 20}}
    encoded = encode_offsets(offsets)
    assert encoded == '{"events":{"0":10,"1":20,"2":30}}'
    assert decode_offsets(encoded) == offsets


def test_decode_empty():
    assert decode_offsets(None) == {}
    assert decode_offsets("") == {}


def test_merge_keeps_untouched_partitions_and_never_goes_backwards():
    committed = {"events": {0: 100, 1: 200, 2: 300}}
    batch = {"events": {1: 250, 2: 290}}  # 290 < 300 must not rewind
    assert merge_offsets(committed, batch) == {"events": {0: 100, 1: 250, 2: 300}}


def test_merge_does_not_mutate_inputs():
    committed = {"events": {0: 1}}
    merge_offsets(committed, {"events": {0: 5}})
    assert committed == {"events": {0: 1}}


def test_merge_new_topic_and_partition():
    assert merge_offsets({}, {"events": {3: 7}}) == {"events": {3: 7}}


def test_latest_committed_skips_foreign_snapshots():
    # newest first: a compaction snapshot (no offsets) sits on top of ours
    summaries = [
        {"operation": "replace"},
        None,
        {OFFSETS_KEY: '{"events":{"0":50}}'},
        {OFFSETS_KEY: '{"events":{"0":10}}'},
    ]
    assert latest_committed_offsets(summaries) == {"events": {0: 50}}


def test_latest_committed_empty_table():
    assert latest_committed_offsets([]) == {}
