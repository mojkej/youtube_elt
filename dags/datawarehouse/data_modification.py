'''
Module for inserting, updating, and deleting data in a PostgreSQL database.'''
import logging

logger = logging.getLogger(__name__)
TABLE_NAME = "yt_api"


def insert_data_to_db(connection, cursor, schema_name, row):
    """
    Insère les données dans la table spécifiée de la base de données PostgreSQL.
    """
    try:
        if schema_name == "staging":
            video_id = "video_id"
            insert_query = f"""
                INSERT INTO {schema_name}.{TABLE_NAME}
                (video_id, title, published_at, duration,
                 view_count, like_count, comment_count)
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s)
                """
            cursor.execute(insert_query, row)
            connection.commit()
            logger.info(
                "Data inserted/updated successfully for row with video_id %s.", row[video_id])
        else:
            video_id = "video_id"
            insert_query = f"""
                INSERT INTO {schema_name}.{TABLE_NAME}
                (video_id, title, published_at, duration,
                 view_count, like_count, comment_count)
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s)
                """
            cursor.execute(insert_query, row)
            connection.commit()
            logger.info("Data inserted row with video_id %s successfully.", {
                        row[video_id]})
    except Exception as e:
        connection.rollback()
        logger.error("Error inserting data with video_id %s: %s",
                     {row[video_id]}, e)
        raise e


def update_data_in_db(connection, cursor, schema_name, row):
    """
    Updates data in the specified table of the PostgreSQL database.
    """
    try:
        if schema_name == "staging":
            update_query = f"""
            UPDATE {schema_name}.{TABLE_NAME}
            SET title = %(title)s,
                published_at = %(published_at)s,
                duration = %(duration)s,
                view_count = %(view_count)s,
                like_count = %(like_count)s,
                comment_count = %(comment_count)s
            WHERE video_id = %(video_id)s;
                """
            cursor.execute(update_query, row)
            connection.commit()
            logger.info("Data updated successfully.")
        else:
            update_query = f"""
                UPDATE {schema_name}.{TABLE_NAME}
                SET title = %(title)s,
                    published_at = %(published_at)s,
                    duration = %(duration)s,
                    view_count = %(view_count)s,
                    like_count = %(like_count)s,
                    comment_count = %(comment_count)s
                WHERE video_id = %(video_id)s AND published_at = %(published_at)s;
                """
            cursor.execute(update_query, row)
            connection.commit()
            logger.info("Data updated successfully.")
    except Exception as e:
        connection.rollback()
        logger.error("Error updating data: %s", e)
        raise e


def delete_data_from_db(connection, cursor, schema_name, video_ids):
    """
    Deletes data from the specified table in the PostgreSQL database 
    based on the provided video_ids.
    """
    try:
        if schema_name == "staging":
            delete_query = f"DELETE FROM {schema_name}.{TABLE_NAME} WHERE video_id = %s;"
            cursor.execute(delete_query, (video_ids,))
            connection.commit()
            logger.info("Data deleted successfully.")
        else:
            delete_query = f"DELETE FROM {schema_name}.{TABLE_NAME} WHERE video_id = %s;"
            cursor.execute(delete_query, (video_ids,))
            connection.commit()
            logger.info("Data deleted successfully.")
    except Exception as e:
        connection.rollback()
        logger.error("Error deleting data: %s", e)
        raise
