from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.video_infos import (
    collect_videos_data,
    get_channel_playlist_id,
    get_videos_id,
    save_videos_to_csv,
)
from data_quality.soda import youtube_data_quality_check
from datawarehouse.database import core_table, staging_table

local_tz = pendulum.timezone("Europe/Paris")

STAGING_SCHEMA = "staging"
CORE_SCHEMA = "core"
# Default arguments for the DAG
default_args = {
    'owner': 'mojkej',
    'email': ['mojkej@gmail.com'],
    "depends_on_past": False,
    'email_on_failure': False,
    'email_on_retry': False,
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=20),
    'retries': 1,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    'retry_delay': timedelta(minutes=10),
}


@dag(
    'produce_csv_youtube',
    default_args=default_args,
    description='DAG to extract YouTube video data',
    schedule_interval='0 5 * * *',
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

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )

    return playlist_id >> videos_ids >> videos_data_list >> csv_result >> trigger_update_db


youtube_extract_dag()


@dag(
    'update_db',
    default_args=default_args,
    description='DAG to update YouTube video data in the database',
    schedule_interval=None,
    catchup=False,
    tags=['youtube', 'datawarehouse'],
)
def youtube_update_db_dag():
    """
    DAG pour extraire les données YouTube avec TaskFlow API
    """
    staging = staging_table()
    core = core_table()
    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality_checks',
    )

    return staging >> core >> trigger_data_quality


youtube_update_db_dag()


@dag(
    'data_quality_checks',
    default_args=default_args,
    description='DAG to perform data quality checks on YouTube video data',
    schedule_interval=None,
    catchup=False,
    tags=['data_quality'],
)
def youtube_data_quality_checks_dag():
    """
    DAG to perform YouTube data quality checks
    """
    data_quality_staging = youtube_data_quality_check(STAGING_SCHEMA)
    data_quality_core = youtube_data_quality_check(CORE_SCHEMA)

    return data_quality_staging >> data_quality_core


youtube_data_quality_checks_dag()
