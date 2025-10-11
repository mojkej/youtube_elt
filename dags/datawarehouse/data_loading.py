import csv
import logging
from datetime import date

logger = logging.getLogger(__name__)


def load_filename():
    """
    Generates a filename based on the current date.
    """
    filename = f"data/youtube_data_{date.today().isoformat()}.csv"

    try:
        logger.info("Generated filename: %s", filename)

        with open(filename, 'r', encoding='utf-8') as csvfile:
            data = csv.reader(csvfile)
            return data
    except FileNotFoundError:
        logger.error("File not found: %s", filename)
    except csv.Error as e:
        logger.error("CSV error: %s", e)
        raise e
