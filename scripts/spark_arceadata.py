from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyiceberg import IcebergTable

# Create a Spark session
spark = SparkSession.builder.appName("IcebergParquetExample").getOrCreate()

# Define the schema for your table
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True)
])

# Specify the path to the table
table_path = "/path/to/your/table"

# Create a PySpark DataFrame
data = [(1, "John"), (2, "Jane"), (3, "Doe")]
df = spark.createDataFrame(data, schema=schema)

# Write the DataFrame to the Iceberg table with Parquet format
df.write.format("iceberg").mode("append").save(table_path)

# Load the Iceberg table
iceberg_table = IcebergTable.load(table_path)

# Perform any necessary operations on the table, such as querying or updating.

# Stop the Spark session
spark.stop()