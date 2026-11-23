"""Inspect the table: row counts, partitions, snapshots, time travel and a duplicate check.

Run with:  docker compose run --rm query
"""

from __future__ import annotations

import sys

from iceberg_setup import IDENTIFIER, get_catalog
from logic import OFFSETS_KEY


def section(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 60 - len(title)))


def main() -> int:
    catalog = get_catalog()
    if not catalog.table_exists(IDENTIFIER):
        print(f"{IDENTIFIER} doesn't exist yet; is the sink running?")
        return 1
    table = catalog.load_table(IDENTIFIER)
    snaps = table.snapshots()
    if not snaps:
        print("Table exists but has no snapshots yet; wait for the first commit.")
        return 1

    con = table.scan().to_duckdb(table_name="events")

    section("Table")
    print("location :", table.location())
    print("spec     :", table.spec())
    print("snapshots:", len(snaps))

    section("Rows per day partition")
    print(
        con.sql("""
        SELECT CAST(event_ts AS DATE) AS event_day, count(*) AS rows
        FROM events GROUP BY 1 ORDER BY 1
    """)
    )

    section("Events by type (last hour)")
    print(
        con.sql("""
        SELECT event_type, count(*) AS n, round(sum(amount), 2) AS revenue
        FROM events
        WHERE event_ts >= now() - INTERVAL 1 HOUR
        GROUP BY 1 ORDER BY n DESC
    """)
    )

    section("Exactly-once check")
    dupes = con.sql("""
        SELECT count(*) FROM (
            SELECT _kafka_partition, _kafka_offset FROM events
            GROUP BY 1, 2 HAVING count(*) > 1
        )
    """).fetchone()[0]
    total = con.sql("SELECT count(*) FROM events").fetchone()[0]
    print(f"rows: {total}   duplicate (partition, offset) pairs: {dupes}")
    print(
        con.sql("""
        SELECT _kafka_partition AS partition, count(*) AS rows,
               min(_kafka_offset) AS min_offset, max(_kafka_offset) AS max_offset
        FROM events GROUP BY 1 ORDER BY 1
    """)
    )

    section("Last 5 snapshots (offsets live in the summary)")
    for s in snaps[-5:]:
        summary = s.summary
        props = dict(getattr(summary, "additional_properties", None) or (summary or {}))
        print(
            f"{s.snapshot_id}  {s.timestamp_ms}  "
            f"added-records={props.get('added-records', '?'):>6}  "
            f"{OFFSETS_KEY}={props.get(OFFSETS_KEY, '-')}"
        )

    section("Time travel: row count as of the first snapshot")
    first = snaps[0]
    n_first = table.scan(snapshot_id=first.snapshot_id).to_arrow().num_rows
    print(f"snapshot {first.snapshot_id}: {n_first} rows   (now: {total})")

    return 0 if dupes == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
