import logging

logger = logging.getLogger(__name__)
TABLE_NAME = "yt_api"


def insert_data_to_db(connection, cursor, schema_name, data):
    """
    Insère les données dans la table spécifiée de la base de données PostgreSQL.
    """
    try:
        if schema_name == "staging":
            for row in data:
                insert_query = f"""
                INSERT INTO {schema_name}.{TABLE_NAME} 
                (video_id, title, published_at, duration, view_count, like_count, comment_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    duration = EXCLUDED.duration,
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count;
                """
                cursor.execute(insert_query, (
                    row['video_id'],
                    row['title'],
                    row['published_at'],
                    row['duration'],
                    row['view_count'],
                    row['like_count'],
                    row['comment_count']
                ))
            connection.commit()
            logger.info("Data inserted/updated successfully.")
        else:
            for row in data:
                insert_query = f"""
                INSERT INTO {schema_name}.{TABLE_NAME} 
                (video_id, title, published_at, duration, view_count, like_count, comment_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    duration = EXCLUDED.duration,
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count;
                """
                cursor.execute(insert_query, (
                    row['video_id'],
                    row['title'],
                    row['published_at'],
                    row['duration'],
                    row['view_count'],
                    row['like_count'],
                    row['comment_count']
                ))
            connection.commit()
            logger.info("Data inserted/updated successfully.")
    except Exception as e:
        connection.rollback()
        logger.error("Error inserting data: %s", e)
        raise e


def update_data_in_db(connection, cursor, schema_name, data):
    """
    Met à jour les données dans la table spécifiée de la base de données PostgreSQL.
    """
    try:
        if schema_name == "staging":
            for row in data:
                update_query = f"""
                UPDATE {schema_name}.{TABLE_NAME}
                SET title = %s,
                    published_at = %s,
                    duration = %s,
                    view_count = %s,
                    like_count = %s,
                    comment_count = %s
                WHERE video_id = %s;
                """
                cursor.execute(update_query, (
                    row['title'],
                    row['published_at'],
                    row['duration'],
                    row['view_count'],
                    row['like_count'],
                    row['comment_count'],
                    row['video_id']
                ))
            connection.commit()
            logger.info("Data updated successfully.")
        else:
            for row in data:
                update_query = f"""
                UPDATE {schema_name}.{TABLE_NAME}
                SET title = %s,
                    published_at = %s,
                    duration = %s,
                    view_count = %s,
                    like_count = %s,
                    comment_count = %s
                WHERE video_id = %s;
                """
                cursor.execute(update_query, (
                    row['title'],
                    row['published_at'],
                    row['duration'],
                    row['view_count'],
                    row['like_count'],
                    row['comment_count'],
                    row['video_id']
                ))
            connection.commit()
            logger.info("Data updated successfully.")
    except Exception as e:
        connection.rollback()
        logger.error("Error updating data: %s", e)
        raise e


def delete_data_from_db(connection, cursor, schema_name, video_ids):
    """
    Supprime les données de la table spécifiée de la base de données PostgreSQL 
    en fonction des video_ids.
    """
    try:
        if schema_name == "staging":
            for video_id in video_ids:
                delete_query = f"DELETE FROM {schema_name}.{TABLE_NAME} WHERE video_id = %s;"
                cursor.execute(delete_query, (video_id,))
            connection.commit()
            logger.info("Data deleted successfully.")
        else:
            for video_id in video_ids:
                delete_query = f"DELETE FROM {schema_name}.{TABLE_NAME} WHERE video_id = %s;"
                cursor.execute(delete_query, (video_id,))
            connection.commit()
            logger.info("Data deleted successfully.")
    except Exception as e:
        connection.rollback()
        logger.error("Error deleting data: %s", e)
        raise
