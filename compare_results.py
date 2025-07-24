import mysql.connector
import pandas as pd

# Setup MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",  # Change this if your password differs
    database="youtube_chat_db"
)

# === WORD COUNT COMPARISON ===

# Load both tables
stream_words = pd.read_sql("SELECT word, count FROM word_counts", conn)
batch_words = pd.read_sql("SELECT word, total_count as count FROM word_counts_batch", conn)

# Clean whitespace just in case
stream_words['word'] = stream_words['word'].str.strip()
batch_words['word'] = batch_words['word'].str.strip()

# Merge on word
merged_words = pd.merge(stream_words, batch_words, on='word', suffixes=('_stream', '_batch'))

# Calculate difference
merged_words['difference'] = merged_words['count_stream'] - merged_words['count_batch']

# Show word comparison
print("\n📌 Word Count Differences (stream vs. batch):")
print(merged_words[merged_words['difference'] != 0])

# === EMOJI COUNT COMPARISON ===

# Load both tables
stream_emojis = pd.read_sql("SELECT emoji, count FROM emoji_counts", conn)
batch_emojis = pd.read_sql("SELECT emoji, total_count as count FROM emoji_counts_batch", conn)

# Clean emojis (important!)
stream_emojis['emoji'] = stream_emojis['emoji'].str.strip()
batch_emojis['emoji'] = batch_emojis['emoji'].str.strip()

# Merge on emoji
merged_emojis = pd.merge(stream_emojis, batch_emojis, on='emoji', suffixes=('_stream', '_batch'))
merged_emojis['difference'] = merged_emojis['count_stream'] - merged_emojis['count_batch']

# Show emoji comparison
print("\n📌 Emoji Usage Differences (stream vs. batch):")
print(merged_emojis[merged_emojis['difference'] != 0])

# Print full tables for debugging
print("\n📄 Streaming Emoji Table:")
print(stream_emojis)

print("\n📄 Batch Emoji Table:")
print(batch_emojis)

