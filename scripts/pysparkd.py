import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession
import pandas as pd
import pyspark, os



# spark = SparkSession.builder \
#       .appName("IcebergTable") \
#       .config("hive.metastore.uris", "thrift://localhost:9083")\
#       .enableHiveSupport() \
#       .getOrCreate()
# df = pd.read_csv("customers.csv")
# dataframe = spark.createDataFrame(df) 

# ## Turn Dataframe into a temporary view
# dataframe.createOrReplaceTempView("myview")

# ## Create new iceberg table in my configured catalog
# spark.sql("CREATE TABLE IF NOT EXISTS icebergtable USING PARQUET AS (SELECT * FROM myview)")


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf() #packages
# conf.set("packages", "iceberg-spark-runtime-3.2_2.12:1.4.2")
# conf.set("packages", "io.delta:delta-core_2.12:2.1.0")
# conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# conf.set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
# conf.set("spark.sql.catalog.spark_catalog.type","hive")

# conf/spark-defaults.conf
# Create catalog sandbox that uses Iceberg's Spark catalog implementation

# Select the warehouse and set a credential for OAuth2 authentication


# # # link to remote metastore
#conf.set("hive.metastore.uris", "thrift://localhost:9083")
# conf.set("spark.hadoop.fs.s3a.session.token", token)
# conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")

# # used to transmit pandas dataframes via arrow to iceberg table
# conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")  

# # Set 'spark_catalog' config for hive

# 
# conf.set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
# conf.set("spark.sql.catalog.local.type","hadoop")

conf.set("spark.sql.catalog.rest_prod", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.rest_prod.type", "rest")
conf.set("spark.sql.catalog.rest_prod.uri", "http://localhost:8080")
conf.set("spark.sql.defaultCatalog","rest_prod")
conf.set("spark.sql.catalog.rest_prod.default-namespace","examples")
#conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

sc = SparkContext( conf=conf)
spark = SparkSession.builder.appName("py sql").getOrCreate()

df = pd.read_csv("customers.csv")
dataframe = spark.createDataFrame(df) 

## Turn Dataframe into a temporary view
dataframe.createOrReplaceTempView("myview")

## Create new iceberg table in my configured catalog
spark.sql("CREATE TABLE IF NOT EXISTS testiceberged USING PARQUET AS (SELECT * FROM myview)")
        

