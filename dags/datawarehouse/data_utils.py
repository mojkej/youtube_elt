from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

ELT_DATABASE_NAME = Variable.get("ELT_DATABASE_NAME")


def get_postgres_connection():
    """
    Establishes and returns a connection to the PostgreSQL database using Airflow's PostgresHook.
    """
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db_elt', database=ELT_DATABASE_NAME)
    connection = pg_hook.get_conn()
    cur = connection.cursor(cursor_factory=RealDictCursor)
    return connection, cur


def close_postgres_connection(connection, cursor):
    """
    Closes the given cursor and connection to the PostgreSQL database.
    """
    cursor.close()
    connection.close()
