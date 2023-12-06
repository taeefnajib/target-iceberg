from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

spark  = SparkSession.builder.enableHiveSupport().getOrCreate()
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

table_path = "demo.nyc.taxis"

df = spark.createDataFrame([], schema)
df.writeTo(table_path).create()


schema = spark.table(table_path).schema
data = [
    (1, 1000371, 1.8, 15.32, "N"),
    (2, 1000372, 2.5, 22.15, "N"),
    (2, 1000373, 0.9, 9.01, "N"),
    (1, 1000374, 8.4, 42.13, "Y")
  ]
df = spark.createDataFrame(data, schema)
df.writeTo(table_path).append()