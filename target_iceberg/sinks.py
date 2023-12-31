"""iceberg target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import BatchSink

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
import os
import math
import sys
import re
import pyspark.sql.functions as F


class icebergSink(BatchSink):
    """iceberg target sink class."""

    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        # Accessing the properties from the target's config
        self.table_name = self.config.get("table_name")
        self.aws_access_key = self.config.get("aws_access_key")
        self.aws_secret_key = self.config.get("aws_secret_key")
        self.aws_region = self.config.get("aws_region")
        self.hive_thrift_uri = self.config.get("hive_thrift_uri")
        self.warehouse_uri = self.config.get("warehouse_uri")
        self.partition_by = self.config.get("partition_by", [])

        # Set the AWS credentials and region as environment variables
        os.environ['AWS_REGION'] = self.aws_region
        os.environ['AWS_ACCESS_KEY_ID'] = self.aws_access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_key


    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
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
        self.rows.append(record)

    def init_spark(self):
        partition_size = (os.cpu_count())*3
        conf = SparkConf() \
            .setAppName("Apache Iceberg with PySpark") \
            .setMaster("local[*]") \
            .setAll([
                ("spark.driver.memory", "4g"),
                ("spark.executor.memory", "4g"),
                ("spark.sql.shuffle.partitions", f"{partition_size}"),
                ('spark.sql.adaptive.coalescePartitions.initialPartitionNum', f"{(os.cpu_count())}"),
                ('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false'),
                ('spark.sql.files.minPartitionNum', "1"),
                ('spark.sql.files.maxPartitionBytes', '500mb'),
              
                # Add Iceberg SQL extensions like UPDATE or DELETE in Spark
                ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

                # Register `hive_catalog`
                ("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog"),
                ('spark.sql.catalog.hive_catalog.type', 'hive'),
                ('spark.sql.catalog.hive_catalog.uri', self.hive_thrift_uri),
                ('spark.sql.catalog.hive_catalog.warehouse', self.warehouse_uri),

                # Configure Warehouse on MinIO
                ("spark.sql.catalog.hive_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
                ("spark.sql.catalog.hive_catalog.s3.path-style-access", "true"),
            ])
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

        return spark
    
    def create_dataframe(self, spark: SparkSession, record: list):
        rows_rdd = spark.sparkContext.parallelize(record)
        rows = rows_rdd.map(lambda x: Row(**x))
        # Function to clean field names
        def clean_field_name(name):
            return re.sub(r'[\s\.,]+', '_', name)
        df = spark.createDataFrame(rows)
        # Rename the columns of the DataFrame
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, clean_field_name(col_name))
        return df
       
    
    def create_table(self, spark: SparkSession, df: DataFrame):
        table_name = f"hive_catalog.default.{self.table_name}"
        
        # Check if the table exists
        if spark.catalog.tableExists(table_name):
            spark.sql(f"REFRESH TABLE {table_name}")
        
        # Retrieve the schema of the DataFrame
        schema = df.schema

        # Build a string of column definitions
        column_definitions = ', '.join([f"{field.name} {field.dataType.simpleString()}" for field in schema.fields])

        # Construct the partition clause based on self.partition_by
        partition_clause = ""
        if self.partition_by:
            partition_keys = ', '.join(self.partition_by)
            partition_clause = f"PARTITIONED BY ({partition_keys})"

        # SQL query to create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
        ) USING iceberg {partition_clause};
        """

        # Execute the query
        spark.sql(create_table_query)
    
    def write_data(self, spark: SparkSession, df: DataFrame):
        table_name = f"hive_catalog.default.{self.table_name}"
        df \
        .writeTo(f"{table_name}") \
        .append()

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        spark = self.init_spark()
        df = self.create_dataframe(spark, self.rows)
        self.create_table(spark, df)
        self.write_data(spark, df)