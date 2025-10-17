from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag
from api.video_infos import (
    collect_videos_data,
    get_channel_playlist_id,
    get_videos_id,
    save_videos_to_csv,
)
from data_quality.soda import youtube_data_quality_check
from datawarehouse.database import core_table, staging_table

local_tz = pendulum.timezone("Europe/Paris")
# Default arguments for the DAG
default_args = {
    'owner': 'mojkej',
    'email': ['mojkej@gmail.com'],
    "depends_on_past": False,
    'email_on_failure': False,
    'email_on_retry': False,
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=300),
    'retries': 1,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    'retry_delay': timedelta(hours=5),
}


@dag(
    'produce_csv_youtube',
    default_args=default_args,
    description='DAG to extract YouTube video data',
    schedule_interval='19 0 * * *',
    # start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['youtube', 'elt', 'csv'],
)
def youtube_extract_dag():
    """
    DAG pour extraire les données YouTube avec TaskFlow API
    """

    playlist_id = get_channel_playlist_id()

    videos_ids = get_videos_id(playlist_id)

    videos_data_list = collect_videos_data(videos_ids)

    csv_result = save_videos_to_csv(videos_data_list)

    return csv_result


# Instantiate the DAG
youtube_extract_dag()


@dag(
    'update_db',
    default_args=default_args,
    description='DAG to update YouTube video data in the database',
    schedule_interval='15 0 * * *',
    catchup=False,
    tags=['youtube', 'datawarehouse'],
)
def youtube_update_db_dag():
    """
    DAG pour extraire les données YouTube avec TaskFlow API
    """
    staging = staging_table()
    core = core_table()

    return staging >> core


@dag(
    'data_quality_checks',
    default_args=default_args,
    description='DAG to perform data quality checks on YouTube video data',
    schedule_interval='15 0 * * *',
    catchup=False,
    tags=['data_quality'],
)
def youtube_data_quality_checks_dag():
    """
    DAG to perform YouTube data quality checks
    """
    data_quality_staging = youtube_data_quality_check("staging")
    data_quality_core = youtube_data_quality_check("core")

    return data_quality_staging >> data_quality_core
