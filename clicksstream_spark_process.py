# Databricks notebook source
# MAGIC %md
# MAGIC ### Connecting spark to cassandra

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


cosmosdb_contact_point = 'host'
cosmosdb_port = '10350'
cosmosdb_username = 'username'
cosmosdb_password = 'pass'
cosmosdb_keyspace = 'clickstream'

# Create the SparkSession with Cassandra configuration
spark = SparkSession.builder \
    .appName("Cassandra Spark Connector Example") \
    .config("spark.cassandra.connection.host", cosmosdb_contact_point) \
    .config("spark.cassandra.auth.username", cosmosdb_username) \
    .config("spark.cassandra.auth.password", cosmosdb_password) \
    .config("spark.cassandra.connection.port", 10350) \
    .config("spark.cassandra.connection.ssl.enabled", "true") \
    .getOrCreate()

# Read data from Cassandra table into DataFrame
df = spark.read\
    .format("org.apache.spark.sql.cassandra") \
    .options(table="clickstream_events", keyspace=cosmosdb_keyspace) \
    .load()

# Show the data
df.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ##Group by "URL" and "country" and calculate the count of unique users

# COMMAND ----------

unique_user_df = df.groupBy("URL" , "country").agg(F.countDistinct("user_id").alias("unique_user_count")).orderBy(F.desc("unique_user_count"))
display(unique_user_df)

# COMMAND ----------

from pyspark.sql import Window
# Define the window specification to partition by user_id and order by timestamp
window_spec = Window.partitionBy("user_id").orderBy("timestamp")


# Calculating the time spent on each URL by subtracting the current timestamp from the next one
time_spent_df = df.select("user_id" , "timestamp" ,"country" ,"url").withColumn("next_timestamp", F.lead("timestamp").over(window_spec))
time_spent_df = time_spent_df.withColumn("time_spent_in_seconds", (F.col("next_timestamp").cast("long") - F.col("timestamp").cast("long")))

time_spent_df = time_spent_df.dropna(subset=["time_spent_in_seconds"])
# Aggregating data by url and country to get average time
time_spent_df = time_spent_df.groupBy("URL" , "country").agg(F.avg("time_spent_in_seconds").alias("average_time_spent"))
display(time_spent_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Processed Data

# COMMAND ----------

final_df = unique_user_df.join(time_spent_df, on=["URL" , "country"])
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert data to Elastic Search
# MAGIC

# COMMAND ----------

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
# Connect to Elasticsearch
es = Elasticsearch(
    cloud_id="cloud_id",
    http_auth=("elastic", "pass")
)
processed_data_pd = final_df.toPandas()
processed_data  = processed_data_pd.to_dict(orient="records")


# Define Elasticsearch index mapping
index_name = "clickstream_data_index"

mapping = {
    "mappings": {
        "properties": {
            "URL": {"type": "keyword"},
            "country": {"type": "keyword"},
            "unique_users": {"type": "integer"},
            "avg_time_spent": {"type": "float"}
        }
    }
}

# Create the index with the defined mapping
try:
    es.indices.create(index=index_name, body=mapping)
except:
    pass

# Bulk index the processed data into Elasticsearch
bulk_data = [
    {
        "_index": index_name,
        "_source": entry
    }
    for entry in processed_data
]

bulk(es, bulk_data)




# COMMAND ----------


