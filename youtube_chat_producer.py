from kafka import KafkaProducer
from googleapiclient.discovery import build
import json
import time

API_KEY = ""
VIDEO_ID = ""

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

youtube = build('youtube', 'v3', developerKey=API_KEY)
video_response = youtube.videos().list(part='liveStreamingDetails', id=VIDEO_ID).execute()
live_chat_id = video_response['items'][0]['liveStreamingDetails']['activeLiveChatId']
next_page_token = None

while True:
    chat_response = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part='snippet,authorDetails',
        pageToken=next_page_token
    ).execute()

    for item in chat_response['items']:
        msg = item['snippet']['displayMessage']
        user = item['authorDetails']['displayName']
        record = {'user': user, 'message': msg}
        print("Sending:", record)
        producer.send('youtube-raw-chat', value=record)

    next_page_token = chat_response.get('nextPageToken')
    time.sleep(5)

