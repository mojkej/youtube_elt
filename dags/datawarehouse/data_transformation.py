import re
from datetime import timedelta


def transform_duration(duration):
    """
    Transforms an ISO 8601 duration string into a PostgreSQL TIME format.
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
    total_duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return total_duration
