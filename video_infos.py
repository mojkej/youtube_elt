'''Module providing video statistics from YouTube API.'''
import os

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_ID")
MAX_RESULTS = 50


def get_channel_playlist_id():
    '''Fetches the channel ID for the given channel.'''
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={
            CHANNEL_HANDLE}&key={API_KEY}'
        response = requests.get(url, timeout=10)
        data = response.json()
        channel_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e


def get_videos_id(playlist_id):
    '''Fetches video details from the given playlist ID.'''
    videos = []
    try:
        url = f'https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={
            playlist_id}&maxResults={MAX_RESULTS}&key={API_KEY}'
        response = requests.get(url, timeout=10)
        data = response.json()
        for item in data.get('items', []):
            video_info = {
                'video_id': item['contentDetails']['videoId']
            }
            videos.append(video_info)
        return videos
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    playlist_id = get_channel_playlist_id()
    get_videos_id(playlist_id)
