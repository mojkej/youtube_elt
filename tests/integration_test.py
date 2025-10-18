import psycopg2
import pytest
import requests


def test_api_response(airflow_variable):
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")
    url = (
        f'https://youtube.googleapis.com/youtube/v3/channels?'
        f'part=contentDetails&forHandle={channel_handle}&key={api_key}'
    )
    try:
        response = requests.get(url, timeout=10)
        assert response.status_code == 200, f"API request failed with status code {response.status_code}"
        data = response.json()
        channel_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        assert channel_playlist_id, "Channel Playlist ID is missing"
    except requests.Timeout:
        pytest.fail("API request timed out")
    except requests.RequestException as e:
        pytest.fail(f"API request failed: {e}")


def test_connection_to_postgres(postgres_connection):
    cursor = None
    try:
        connection = postgres_connection
        cursor = connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        assert result[0] == 1, "Failed to execute test query on PostgreSQL"
    except psycopg2.Error as e:
        pytest.fail(f"PostgreSQL connection failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
