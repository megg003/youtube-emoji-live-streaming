from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, current_timestamp
from pyspark.sql.types import StructType, StringType
from kafka import KafkaProducer
import json

# Kafka Producer to send word counts
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Spark Session
spark = SparkSession.builder \
    .appName("YouTubeChatStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
            "mysql:mysql-connector-java:8.0.29") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema for JSON
from pyspark.sql.types import StructType, StringType

schema = StructType() \
    .add("user", StringType()) \
    .add("message", StringType()) \
    .add("emojis", StringType())  # ✅ This is the correct format


# Kafka stream from cleaned chat
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "youtube-cleaned-chat") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

words = json_df.select(explode(split(col("message"), " ")).alias("word"))
word_counts = words.groupBy("word").count()
word_counts_with_time = word_counts.withColumn("timestamp", current_timestamp())
emoji_df = json_df.select(explode(split(col("emojis"), "")).alias("emoji"))
emoji_counts = emoji_df.groupBy("emoji").count()
emoji_counts_with_time = emoji_counts.withColumn("timestamp", current_timestamp())


# MySQL connection
mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "",
    "user": "",
    "password": "",
    "dbtable": "word_counts"
}

# Batch writer function
# Batch writer function for word counts
def process_batch(batch_df, batch_id):
    print(f"\n===== Word Batch ID: {batch_id} =====")
    if not batch_df.rdd.isEmpty():
        batch_df.show(truncate=False)

        batch_df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .options(**mysql_properties) \
            .save()

        print(f"Word batch {batch_id} written to MySQL")

        for row in batch_df.collect():
            record = {
                'word': row['word'],
                'count': row['count'],
                'timestamp': str(row['timestamp'])
            }
            kafka_producer.send('youtube-word-counts', value=record)
            print("Sent Word to Kafka:", record)


# Batch writer function for emoji counts
def process_emoji_batch(batch_df, batch_id):
    print(f"\n===== Emoji Batch ID: {batch_id} =====")
    if not batch_df.rdd.isEmpty():
        batch_df.show(truncate=False)

        batch_df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .options(
                url="jdbc:mysql://localhost:3306/youtube_chat_db",
                driver="com.mysql.cj.jdbc.Driver",
                dbtable="emoji_counts",
                user="root",
                password="root"
            ) \
            .save()

        print(f"Emoji batch {batch_id} written to MySQL")

        for row in batch_df.collect():
            record = {
                'emoji': row['emoji'],
                'count': row['count'],
                'timestamp': str(row['timestamp'])
            }
            kafka_producer.send('youtube-emoji-counts', value=record)
            print("Sent Emoji to Kafka:", record)


# Start both word and emoji streams
query = word_counts_with_time.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .start()

emoji_query = emoji_counts_with_time.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_emoji_batch) \
    .start()
import time

start_time = time.time()  


print("⏱️ Streaming Duration:", time.time() - start_time, "seconds")


# Wait for both streams
query.awaitTermination()
emoji_query.awaitTermination()

