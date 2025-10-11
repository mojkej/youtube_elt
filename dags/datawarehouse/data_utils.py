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


def create_schema(schema_name):
    """
    Creates a new schema in the PostgreSQL database if it does not already exist.
    """
    connection, cursor = get_postgres_connection()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_postgres_connection(connection, cursor)


def create_table(schema_name, table_name, table_sql):
    """
    Creates a table in the PostgreSQL database if it does not already exist.
    """
    connection, cursor = get_postgres_connection()
    table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        video_id VARCHAR(11) PRIMARY KEY NOT NULL,
        title TEXT NOT NULL,
        published_at TIMESTAMP NOT NULL,
        view_count INT,
        like_count INT,
        comment_count INT,
        duration TIME NOT NULL,);"""
    try:
        if schema_name == "staging":
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({table_sql});")
        else:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({table_sql});")
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_postgres_connection(connection, cursor)


def get_videos_ids_from_db(schema_name, table_name):
    """
    Retrieves all video IDs from the specified table in the PostgreSQL database.
    """
    connection, cursor = get_postgres_connection()
    try:
        cursor.execute(f"SELECT video_id FROM {schema_name}.{table_name};")
        rows = cursor.fetchall()
        video_ids = [row['video_id'] for row in rows]
        return video_ids
    except Exception as e:
        raise e
    finally:
        close_postgres_connection(connection, cursor)
