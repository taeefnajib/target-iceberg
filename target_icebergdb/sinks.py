"""icebergdb target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import BatchSink
from pandas import DataFrame
from pyspark.sql import SparkSession
import pandas as pd
import pyspark, os

from datetime import datetime

class icebergdbSink(BatchSink):
    """icebergdb target sink class."""
    def __init__(self, target, schema, stream_name,key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )


    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        batch_key = context["batch_id"]
        self.rows = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """

        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)
        self.rows.append(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy
        conf = (
            pyspark.SparkConf()
                .setAppName('IcebergSparkApp')
        )
        spark = SparkSession.builder.config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().config(conf=conf).getOrCreate()
        df = pd.DataFrame(self.rows)
        dataframe = spark.createDataFrame(df) 

        ## Turn Dataframe into a temporary view
        dataframe.createOrReplaceTempView("myview")

        ## Create new iceberg table in my configured catalog
        spark.sql("CREATE TABLE IF NOT EXISTS icebergtable USING PARQUET AS (SELECT * FROM myview)")
        

