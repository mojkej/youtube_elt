'''Module providing video statistics from YouTube API.'''
import csv
import os
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
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
                video_data = {
                    'video_id': item['contentDetails']['videoId']
                }
                videos_ids.append(video_data)
            page_token = data.get('nextPageToken')
            if not page_token:
                break
            url = f'https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={
                playlist_id}&maxResults={MAX_RESULTS}&pageToken={page_token}&key={API_KEY}'
    except requests.exceptions.RequestException as e:
        raise e
    return videos_ids


def extract_videos_infos(videos_ids):
    '''Extracts detailed statistics for each video ID using a generator.'''
    for video in videos_ids:
        video_id = video['video_id']
        url = f'https://youtube.googleapis.com/youtube/v3/videos?part=ContentDetails&id={
            video_id}&key={API_KEY}&part=snippet&part=statistics'
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if 'items' in data and data['items']:
                item = data['items'][0]
                video_data = {
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt'],
                    'duration': item['contentDetails']['duration'],
                    'view_count': int(item['statistics'].get('viewCount', 0)),
                    'like_count': int(item['statistics'].get('likeCount', 0)),
                    'comment_count': int(item['statistics'].get('commentCount', 0))
                }
                yield video_data
        except requests.exceptions.RequestException as e:
            raise e


def save_videos_to_csv(videos_generator, filename='videos_data.csv'):
    '''Saves video data from generator to CSV file.'''
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['video_id', 'title', 'published_at', 'duration',
                      'view_count', 'like_count', 'comment_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Écrire l'en-tête
        writer.writeheader()

        # Écrire chaque ligne au fur et à mesure
        video_count = 0
        for video_data in videos_generator:
            writer.writerow(video_data)
            video_count += 1
            print(f"Vidéo {video_count} sauvegardée: {video_data['title']}")

        print(f"\nTotal de {video_count} vidéos sauvegardées dans {filename}")


if __name__ == "__main__":
    videos_data = get_videos_id(get_channel_playlist_id())

    # Sauvegarder directement en CSV avec le générateur
    save_videos_to_csv(extract_videos_infos(
        videos_data), f'data/youtube_videos_{CHANNEL_HANDLE}_{date.today()}.csv')
