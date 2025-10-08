'''Module providing video statistics from YouTube API.'''
import os

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_ID")


def get_channel_playlist_id():
    '''Fetches the channel ID for the given channel.'''
    try:
        URL = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={
            CHANNEL_HANDLE}&key={API_KEY}'
        response = requests.get(URL, timeout=10)
        data = response.json()
        channel_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    get_channel_playlist_id()
