import re
from datetime import datetime, timedelta


def parse_duration(duration):
    """
    Parses an ISO 8601 duration string and returns a timedelta object.
    """
    pattern = re.compile(
        r'PT'              # Start with 'PT'
        r'(?:(\d+)H)?'    # Hours
        r'(?:(\d+)M)?'    # Minutes
        r'(?:(\d+)S)?'    # Seconds
    )
    match = pattern.fullmatch(duration)
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration format: {duration}")

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def transform_data(row):
    """
    Transforms a data row by parsing the duration field.
    """
    duration = parse_duration(row['duration'])
    # Convert to time object
    row['duration'] = (datetime.min + duration).time()
    row['video_type'] = 'Shorts' if duration.total_seconds() <= 600 else 'Standard'
    return row
