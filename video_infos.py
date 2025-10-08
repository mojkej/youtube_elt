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
    videos_ids = []
    page_token = None
    url = f'https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={
        playlist_id}&maxResults={MAX_RESULTS}&key={API_KEY}'
    try:
        while True:
            if page_token:
                url += f'&pageToken={page_token}'
            response = requests.get(url, timeout=10)
            data = response.json()
            for item in data.get('items', []):
                video_info = {
                    'video_id': item['contentDetails']['videoId']
                }
                videos_ids.append(video_info)
            page_token = data.get('nextPageToken')
            if not page_token:
                break
            url = f'https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={
                playlist_id}&maxResults={MAX_RESULTS}&pageToken={page_token}&key={API_KEY}'
    except requests.exceptions.RequestException as e:
        raise e
    return videos_ids


if __name__ == "__main__":
    get_videos_id(get_channel_playlist_id())
