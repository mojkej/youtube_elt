import logging

from airflow.operators.bash import BashOperator

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"


def youtube_data_quality_check(schema):
    """
    This function creates a BashOperator task to perform data quality checks using Soda SQL.
    It checks the data quality of the 'youtube_videos' table in the PostgreSQL database.

    Args:
        dag: The Airflow DAG to which the task will be added.
    """
    try:
        task = BashOperator(
            task_id="youtube_data_quality_check",
            bash_command=f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configurations.yml -v {schema} {SODA_PATH}/checks.yml"
        )
        return task
    except Exception as e:
        logging.error(
            f"Error creating data quality check task for schema : {schema}")
        raise e
