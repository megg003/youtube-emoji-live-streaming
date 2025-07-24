from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YouTubeChatBatchAnalysis") \
    .config("spark.jars.packages", 
            "mysql:mysql-connector-java:8.0.29") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# MySQL connection options
mysql_options = {
    "url": "",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "",
    "password": ""
}

# === Load Word Counts Table ===
word_df = spark.read \
    .format("jdbc") \
    .options(**mysql_options, dbtable="word_counts") \
    .load()

# === Load Emoji Counts Table ===
emoji_df = spark.read \
    .format("jdbc") \
    .options(**mysql_options, dbtable="emoji_counts") \
    .load()

print("\n==== Top 20 Most Frequent Words ====")
word_df.groupBy("word") \
    .agg(_sum("count").alias("total_count")) \
    .orderBy(desc("total_count")) \
    .show(20, truncate=False)

print("\n==== Top 20 Most Used Emojis ====")
emoji_df.groupBy("emoji") \
    .agg(_sum("count").alias("total_count")) \
    .orderBy(desc("total_count")) \
    .show(20, truncate=False)

# Optional: Save batch results to CSV files for report
# Save batch word counts to new MySQL table
word_df.groupBy("word") \
    .agg(_sum("count").alias("total_count")) \
    .orderBy(desc("total_count")) \
    .write \
    .format("jdbc") \
    .option("url", mysql_options["url"]) \
    .option("driver", mysql_options["driver"]) \
    .option("dbtable", "word_counts_batch") \
    .option("user", mysql_options["user"]) \
    .option("password", mysql_options["password"]) \
    .mode("overwrite") \
    .save()

# Save batch emoji counts to new MySQL table
emoji_df.groupBy("emoji") \
    .agg(_sum("count").alias("total_count")) \
    .orderBy(desc("total_count")) \
    .write \
    .format("jdbc") \
    .option("url", mysql_options["url"]) \
    .option("driver", mysql_options["driver"]) \
    .option("dbtable", "emoji_counts_batch") \
    .option("user", mysql_options["user"]) \
    .option("password", mysql_options["password"]) \
    .mode("overwrite") \
    .save()
import time

start_time = time.time()  
print("⏱️ Batch Processing Duration:", time.time() - start_time, "seconds")

