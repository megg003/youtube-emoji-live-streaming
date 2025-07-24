# compare_results.py

import mysql.connector
import pandas as pd

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",  # change if needed
    database="youtube_chat_db"
)

# ==== Load Streaming Word Counts ====
stream_words = pd.read_sql("SELECT word, count FROM word_counts", conn)

# ==== Load Batch Word Counts ====
batch_words = pd.read_sql("SELECT word, total_count as count FROM word_counts_batch", conn)

# ==== Compare Word Counts ====
word_merge = pd.merge(stream_words, batch_words, on="word", suffixes=("_stream", "_batch"))
word_merge["difference"] = word_merge["count_stream"] - word_merge["count_batch"]

print("\n📌 Word Count Differences (stream vs. batch):")
print(word_merge[word_merge["difference"] != 0].sort_values(by="difference", ascending=False))


# ==== Load Streaming Emoji Counts ====
stream_emojis = pd.read_sql("SELECT emoji, count FROM emoji_counts", conn)

# ==== Load Batch Emoji Counts ====
batch_emojis = pd.read_sql("SELECT emoji, total_count as count FROM emoji_counts_batch", conn)

# ==== Compare Emoji Counts ====
emoji_merge = pd.merge(stream_emojis, batch_emojis, on="emoji", suffixes=("_stream", "_batch"))
emoji_merge["difference"] = emoji_merge["count_stream"] - emoji_merge["count_batch"]

print("\n📌 Emoji Usage Differences (stream vs. batch):")
print(emoji_merge[emoji_merge["difference"] != 0].sort_values(by="difference", ascending=False))


# Optional: Save results for report
word_merge.to_csv("batch_results/word_comparison.csv", index=False)
emoji_merge.to_csv("batch_results/emoji_comparison.csv", index=False)

conn.close()

