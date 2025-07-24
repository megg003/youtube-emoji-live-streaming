
# YouTube Emoji Live Streaming 

A real-time and batch processing pipeline that captures, cleans, and analyzes YouTube Live Chat data — especially emoji-based reactions — using Apache Kafka and Apache Spark.

##  Project Overview

This project uses the YouTube Live Chat API to stream real-time messages from a live video, and processes them using Big Data technologies. It highlights:

- **Real-time (Streaming) Analysis** using Kafka + Spark
- **Batch Mode Analysis** for trend evaluation
- **Emoji & Word Count** to evaluate user sentiment
- **MySQL** used for storing and comparing results

## Tech Stack

- **Apache Kafka**: For real-time message streaming
- **Apache Spark**: For micro-batch processing
- **Python**: For producers and consumers
- **MySQL**: For storing processed data
- **YouTube Data API**: For live chat retrieval

## Setup

### 1. Install Dependencies

Ensure you have Kafka, Spark, and Python libraries (`pyspark`, `kafka-python`, `mysql-connector-python`) installed.

### 2. Create Kafka Topics

```bash
bin/kafka-topics.sh --create --topic youtube-raw-chat --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic youtube-cleaned-chat --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic youtube-word-counts --bootstrap-server localhost:9092
