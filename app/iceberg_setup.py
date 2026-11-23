"""Catalog connection, bucket bootstrap and table definition shared by every script."""

from __future__ import annotations

import logging
import os
import time
from urllib.parse import urlparse

from pyarrow.fs import FileType, S3FileSystem
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

log = logging.getLogger(__name__)

NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "demo")
TABLE_NAME = os.environ.get("ICEBERG_TABLE", "events")
IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "password")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")

SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "user_id", LongType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "amount", DoubleType(), required=False),
    NestedField(5, "event_ts", TimestamptzType(), required=True),
    # Lineage columns: where in Kafka each row came from. They make duplicates
    # detectable (see query.py) and debugging much easier.
    NestedField(6, "_kafka_partition", IntegerType(), required=True),
    NestedField(7, "_kafka_offset", LongType(), required=True),
    NestedField(8, "_ingested_at", TimestamptzType(), required=True),
)

# Partition by the event's day (hidden partitioning: queries filter on event_ts,
# Iceberg prunes files; nobody has to know a partition column exists).
PARTITION_SPEC = PartitionSpec(PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="event_day"))


def get_catalog() -> Catalog:
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os.environ.get("CATALOG_URI", "http://localhost:8181"),
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
        },
    )


def ensure_bucket(timeout_s: float = 90) -> None:
    """Create the warehouse bucket if needed (no separate `mc` container required)."""
    url = urlparse(S3_ENDPOINT)
    fs = S3FileSystem(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        region=S3_REGION,
        endpoint_override=url.netloc,
        scheme=url.scheme or "http",
        allow_bucket_creation=True,
    )
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if fs.get_file_info(BUCKET).type == FileType.NotFound:
                fs.create_dir(BUCKET)
                log.info("created bucket s3://%s", BUCKET)
            return
        except OSError as e:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for object storage at %s (%s)", S3_ENDPOINT, e)
            time.sleep(2)


def ensure_table(catalog: Catalog) -> Table:
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog.create_table_if_not_exists(
        IDENTIFIER,
        schema=SCHEMA,
        partition_spec=PARTITION_SPEC,
        properties={"format-version": "2"},
    )
