import datetime
from gridflow.features.calendar import is_weekend

def test_weekend():
    d = datetime.date(2021,10,9)  # Saturday
    assert is_weekend(d)

def test_weekday():
    d = datetime.date(2021,10,11)  # Monday
    assert not is_weekend(d)
