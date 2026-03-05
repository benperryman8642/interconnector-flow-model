from datetime import datetime
from gridflow.common.time import ensure_utc

def test_ensure_utc_naive():
    dt = datetime(2020,1,1,0,0,0)
    dt2 = ensure_utc(dt)
    assert dt2.tzinfo is not None


def test_ensure_utc_aware():
    import datetime as dt
    aware = dt.datetime(2020,1,1,0,0,0, tzinfo=dt.timezone.utc)
    assert ensure_utc(aware).tzinfo is not None
