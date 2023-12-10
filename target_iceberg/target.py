"""iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    icebergSink,
)


class Targeticeberg(Target):
    """Sample target for icebergdb."""

    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "table_name",
            th.StringType,
            description="Name of iceberg table",
            required=True,
        ),
        th.Property(
            "aws_access_key",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS S3 bucket access key",
            required=True
        ),
        th.Property(
            "aws_secret_key",
            th.StringType,
            secret=True,
            description="AWS S3 bucket secret key",
            required=True
        ),
        th.Property(
            "aws_region",
            th.StringType,
            description="AWS region for S3 bucket",
            required=True
        ),
        th.Property(
            "hive_thrift_uri",
            th.StringType,
            description="URI for Hive Thrift service",
            required=True
        ),
        th.Property(
            "warehouse_uri",
            th.StringType,
            description="URI for the data warehouse (e.g., S3 bucket path)",
            required=True
        ),
        th.Property(
            "partition_by",
            th.ArrayType(th.StringType),
            description="List of column names to partition the table by",
            required=False
        ),
    ).to_dict()

    default_sink_class = icebergSink


if __name__ == "__main__":
    Targeticeberg.cli()