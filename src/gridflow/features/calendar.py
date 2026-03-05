"""Calendar flags"""
import datetime

def is_weekend(dt: datetime.date) -> bool:
    return dt.weekday() >= 5
