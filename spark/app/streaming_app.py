from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

spark = SparkSession.builder \
    .appName("RealTimePipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.master", "local[2]") \
    .getOrCreate()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# FIXED SparkSession for Docker
spark = SparkSession.builder \
    .appName("RealTimePipeline") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Schema exact match
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("page_url", StringType()),
    StructField("event_type", StringType())
])

# Read Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"]) \
    .option("subscribe", "user_activity") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse to DF
df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*").withColumn("event_time", to_timestamp("event_time"))

# Watermark 2min
dfw = df.withWatermark("event_time", "2 minutes")

# 1. Page views: 1min tumbling, page_url count where page_view
page_views = dfw \
    .filter(col("event_type") == "page_view") \
    .groupBy(window("event_time", "1 minute").alias("window"), "page_url") \
    .agg(count("*").alias("view_count")) \
    .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), "page_url", "view_count")

# 2. Active users: 5min sliding every 1min, approx distinct users
active_users = dfw \
    .groupBy(window("event_time", "5 minutes", "1 minute").alias("window")) \
    .agg(countDistinct("user_id").alias("active_user_count")) \
    .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), "active_user_count")

# 3. User sessions: Stateful (simplified with dropDuplicates or approx; full flatMapGroupsWithState for prod [web:3])
# For session: filter start/end, compute duration on end events (approx, assume paired)
sessions = dfw.filter(col("event_type").isin("session_start", "session_end")) \
    .groupBy("user_id").agg(
        min(when(col("event_type")=="session_start", "event_time")).alias("session_start_time"),
        max(when(col("event_type")=="session_end", "event_time")).alias("session_end_time")
    ).withColumn("session_duration_seconds", 
                 unix_timestamp("session_end_time") - unix_timestamp("session_start_time"))

# Enriched for Kafka
enriched = df.withColumn("processing_time", current_timestamp().cast("string"))

# Raw to Parquet lake, partition by date(event_time)
lake_query = df \
    .withColumn("event_date", date_format("event_time", "yyyy-MM-dd")) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .partitionBy("event_date") \
    .option("path", "/opt/spark/data/lake") \
    .option("checkpointLocation", "/opt/spark/checkpoint/lake") \
    .trigger(processingTime="10 seconds") \
    .start()

# Postgres sinks (use foreachBatch for upsert)
db_props = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

def upsert_batch(df, epoch_id, table):
    db_url = os.environ["DB_URL"]
    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .jdbc(db_url, table, db_props)

page_query = page_views.writeStream \
    .foreachBatch(lambda df, id: upsert_batch(df, id, "page_view_counts")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/opt/spark/checkpoint/page_views") \
    .trigger(processingTime="30 seconds") \
    .start()

active_query = active_users.writeStream \
    .foreachBatch(lambda df, id: upsert_batch(df, id, "active_users")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/opt/spark/checkpoint/active_users") \
    .trigger(processingTime="30 seconds") \
    .start()

session_query = sessions.writeStream \
    .foreachBatch(lambda df, id: upsert_batch(df, id, "user_sessions")) \
    .outputMode("update") \
    .option("checkpointLocation", "/opt/spark/checkpoint/sessions") \
    .trigger(processingTime="30 seconds") \
    .start()

# Enriched Kafka
kafka_enriched = enriched.selectExpr("to_json(struct(*)) as value")
enriched_query = kafka_enriched.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"]) \
    .option("topic", "enriched_activity") \
    .option("checkpointLocation", "/opt/spark/checkpoint/enriched") \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()
