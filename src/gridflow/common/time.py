"""UTC handling and basic helpers"""
from datetime import datetime, timezone

def ensure_utc(dt: datetime) -> datetime:
    """Return an aware datetime in UTC. If naive, assume it's UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
