import logging

from airflow.decorators import task

from datawarehouse.data_loading import load_filename
from datawarehouse.data_modification import (
    delete_data_from_db,
    insert_data_to_db,
    update_data_in_db,
)
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_utils import (
    close_postgres_connection,
    create_schema,
    create_table,
    get_postgres_connection,
    get_videos_ids_from_db,
)

logger = logging.getLogger(__name__)
TABLE_NAME = "yt_api"


@task
def staging_table():
    """Create and populate the staging table.

    Raises:
        e: If an error occurs during staging table operations.
    """
    schema = "staging"
    conn, cur = None, None
    try:
        connect, cur = get_postgres_connection()
        data = load_filename()
        create_schema(schema)
        create_table(schema)

        table_ids = get_videos_ids_from_db(schema)

        for row in data:
            if len(table_ids) == 0:
                insert_data_to_db(connect, cur, schema, row)
            else:
                if row['video_id'] in table_ids:
                    update_data_in_db(connect, cur, schema, row)
                else:
                    insert_data_to_db(connect, cur, schema, row)
        video_ids_csv = {row['video_id'] for row in data}
        videos_ids_delete = set(table_ids) - video_ids_csv
        if videos_ids_delete:
            delete_data_from_db(connect, cur, schema, videos_ids_delete)

        logger.info("Staging table operations completed successfully.")
    except Exception as e:
        logger.error("Error in staging table operations: %s", e)
        raise e
    finally:
        if conn and cur:
            close_postgres_connection(connect, cur)


@task
def core_table():
    """Create and populate the core table.
    Raises:
        e: If an error occurs during core table operations.
    """
    schema = "core"
    conn, cur = None, None
    try:
        conn, cur = get_postgres_connection()
        create_schema(schema)
        create_table(schema)

        table_ids = get_videos_ids_from_db(schema)
        current_videos_ids = set()

        cur.execute(f"SELECT * FROM staging.{TABLE_NAME};")
        rows = cur.fetchall()  # because we don't have a large amount of data
        for row in rows:
            current_videos_ids.add(row['video_id'])
            if len(table_ids) == 0:
                transform_row = transform_data(row)
                insert_data_to_db(conn, cur, schema, transform_row)
            else:
                transform_row = transform_data(row)

                if transform_row['video_id'] in table_ids:
                    update_data_in_db(conn, cur, schema, transform_row)
                else:
                    insert_data_to_db(conn, cur, schema, transform_row)
        videos_ids_delete = set(table_ids) - current_videos_ids
        if videos_ids_delete:
            delete_data_from_db(conn, cur, schema, videos_ids_delete)

        logger.info("Core table operations completed successfully.")
    except Exception as e:
        logger.error("Error in core table operations: %s", e)
        raise e
    finally:
        if conn and cur:
            close_postgres_connection(conn, cur)
