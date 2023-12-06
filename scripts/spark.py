from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
#

catalog = load_catalog(
    "docs",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)

# if "docs_example" not in catalog.list_namespaces():
#    catalog.create_namespace("docs_example")

print(catalog.list_namespaces)
print(catalog.list_tables("nyc"))



schema = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="first_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="last_name", field_type=StringType(), required=False),
    NestedField(field_id=5, name="email", field_type=StringType(), required=False),
    NestedField(field_id=6, name="ip_address", field_type=StringType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)
sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

print(catalog.list_tables("docs_example"))

if ('docs_example', 'test_customers') in catalog.list_tables("docs_example"):
  print("INFO: TABLE ALREADY EXISTS")
  pass
else:
  catalog.create_table(
      identifier="docs_example.test_customers",
      schema=schema,
      partition_spec=partition_spec,
      sort_order=sort_order,
  )

from pyarrow import csv
fn = 'scripts/customers.csv'
table = csv.read_csv(fn)
print(type(table))



from py4j.java_gateway import JavaGateway, java_import

# Connect to the JVM
gateway = JavaGateway()

# Import Iceberg classes
java_import(gateway.jvm, "org.apache.iceberg.*")

# Specify the path to your existing or new Iceberg table
iceberg_table_path = "docs_example.test_customers"

# Path to your Parquet file or any other source
parquet_file_path = fn

# Open the Iceberg table
table = gateway.jvm.Table.open(iceberg_table_path)

# Read data from Parquet file into Iceberg table
table.newAppend().appendFile(parquet_file_path).commit()

print("Data written to the Iceberg table successfully.")







# table = catalog.load_table("docs_example.test_customers").to_arrow()
# print(type(table))

# import os
# from pyspark.sql import SparkSession, DataFrame
# import pyspark.sql.functions as F

# def write_data(spark: SparkSession):
#     current_dir = "../meltanoprojects/speedrun/data_source"
#     path = os.path.join(current_dir, "customers.csv")

#     vaccinations: DataFrame = spark.read \
#       .option("header", "true") \
#       .option("inferSchema", "true") \
#       .csv(path)

#     vaccinations \
#       .writeTo("docs_example.test_customers") \
#       .append()

# spark  = SparkSession.builder.enableHiveSupport().getOrCreate()
# print("\n\n[INFO] TABLE CREATED")
# write_data(spark)






'''This throws some error
from pyspark.sql import SparkSession


spark  = SparkSession.builder.enableHiveSupport().getOrCreate()
print(spark)

from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

df = spark.createDataFrame([], schema)

df.writeTo("docs_example.taxis").create()
'''




# import os
# from pyspark.sql import SparkSession, DataFrame
# import pyspark.sql.functions as F

# def create_table(spark: SparkSession):
#     spark.sql("""
#       CREATE TABLE IF NOT EXISTS docs_example.vaccinations (
#         location string,
#         date date,
#         vaccine string,
#         source_url string,
#         total_vaccinations bigint,
#         people_vaccinated bigint,
#         people_fully_vaccinated bigint,
#         total_boosters bigint
#       ) USING iceberg PARTITIONED BY (location, date)
#     """)
    
# def write_data(spark: SparkSession):
#     current_dir = os.path.realpath(os.path.dirname(__file__))
#     path = os.path.join(current_dir, "covid19-vaccinations-country-data", "Belgium.csv")

#     vaccinations: DataFrame = spark.read \
#       .option("header", "true") \
#       .option("inferSchema", "true") \
#       .csv(path)

#     vaccinations \
#       .withColumn("date", F.to_date(F.col("date"))) \
#       .writeTo("my_iceberg_catalog.vaccinations") \
#       .append()

# spark  = SparkSession.builder.enableHiveSupport().getOrCreate()
# create_table(spark)
# print("\n\n[INFO] TABLE CREATED")
# write_data(spark)