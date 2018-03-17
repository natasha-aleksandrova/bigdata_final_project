import json
import uuid
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import json_tuple, from_json, get_json_object, col, desc
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

category_schema = StructType().add("shortname", StringType()).add("sort_name", StringType()).add("id", IntegerType()).add("name", StringType())
group_schema = StructType().add("group_country", StringType())
schema = StructType().add("category", category_schema).add("group", group_schema).add("response", StringType())

spark = SparkSession.builder.master("local").appName("Spark-Kafka-Integration").getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.128.0.8:9092") \
    .option("subscribe", "rsvps2") \
    .option("startingOffsets", "earliest") \
    .load()
df.printSchema()


df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
df.printSchema()


# df = df.groupBy("parsed_value.category.shortname", "parsed_value.group.group_country",
#                "parsed_value.response").count().orderBy("shortname", desc("count"))
# query = df \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()


query = df.writeStream.format("parquet").option(
    "checkpointLocation", "hdfs://spark-zeppelin-m/rsvp_checkpoint" + str(uuid.uuid4())).option(
    "path", "hdfs://spark-zeppelin-m/rsvp").start()

query.awaitTermination()

spark.stop()

