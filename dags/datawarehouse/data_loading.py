import csv
import logging
from datetime import date

logger = logging.getLogger(__name__)


def load_filename():
    """
    Generates a filename based on the current date and returns a list of dicts (rows).
    Returns [] if the file does not exist.
    """
    filename = f"./data/youtube_videos_Squeezie_{date.today().isoformat()}.csv"

    try:
        logger.info("Generated filename: %s", filename)
        with open(filename, 'r', encoding='utf-8') as csvfile:
            # fully materialize the content before closing the file
            reader = csv.DictReader(csvfile)
            data = list(reader)
            return data
    except FileNotFoundError:
        logger.error("File not found: %s", filename)
        return []
    except csv.Error as e:
        logger.error("CSV error: %s", e)
        raise e
