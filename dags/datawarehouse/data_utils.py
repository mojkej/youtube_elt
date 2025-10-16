from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

ELT_DATABASE_NAME = "elt_db"
TABLE_NAME = "yt_api"
POSTGRES_DB_CONN_ID = "postgres_db_yt_elt"


def get_postgres_connection():
    """
    Establishes and returns a connection to the PostgreSQL database using Airflow's PostgresHook.
    """
    pg_hook = PostgresHook(
        postgres_conn_id=POSTGRES_DB_CONN_ID, database=ELT_DATABASE_NAME)
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


def create_table(schema_name):
    """
    Crée la table dans le schéma donné. S'assure que le schéma existe et utilise
    uniquement la définition des colonnes (pour éviter d'imbriquer CREATE TABLE).
    """
    connection, cursor = get_postgres_connection()
    try:
        # s'assurer que le schema existe
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        # définitions des colonnes (sans CREATE TABLE)
        table_staging_columns = """
            video_id VARCHAR(11) PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            view_count INT,
            like_count INT,
            comment_count INT,
            duration VARCHAR(20) NOT NULL
        """
        table_core_columns = """
            video_id VARCHAR(11) PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            view_count INT,
            like_count INT,
            comment_count INT,
            duration TIME NOT NULL
        """

        cols = table_staging_columns if schema_name == "staging" else table_core_columns
        create_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{TABLE_NAME} ({cols});"
        cursor.execute(create_sql)
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_postgres_connection(connection, cursor)


def get_videos_ids_from_db(schema_name):
    """
    Retrieves all video IDs from the specified table in the PostgreSQL database.
    """
    connection, cursor = get_postgres_connection()
    try:
        cursor.execute(f"SELECT video_id FROM {schema_name}.{TABLE_NAME};")
        rows = cursor.fetchall()
        video_ids = [row['video_id'] for row in rows]
        return video_ids
    except Exception as e:
        raise e
    finally:
        close_postgres_connection(connection, cursor)
