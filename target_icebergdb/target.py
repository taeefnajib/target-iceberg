"""icebergdb target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_icebergdb.sinks import (
    icebergdbSink,
)


class Targeticebergdb(Target):
    """Sample target for icebergdb."""

    name = "target-icebergdb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "table_name",
            th.StringType,
            description="name of iceberg table",
        ),
        th.Property(
            "auth_token",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="The path to the target output file",
        ),
    ).to_dict()

    default_sink_class = icebergdbSink


if __name__ == "__main__":
    Targeticebergdb.cli()
