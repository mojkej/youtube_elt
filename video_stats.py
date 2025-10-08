'''Module providing video statistics from YouTube API.'''
import os

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

URL = f'https://youtube.googleapis.com/youtube/v3/videos?part=ContentDetails&id={
    CHANNEL_ID}&key={API_KEY}'

response = requests.get(URL, timeout=10)
data = response.json()
print(data)
