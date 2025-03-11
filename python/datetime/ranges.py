import datetime


def daterange(
    start_date: datetime.date, end_date: datetime.date, step=1
) -> typing.Iterator[str]:
    """Generator for datetime returning the date as iso formatted string"""
    dt = start_date
    while dt <= end_date:
        yield dt.isoformat()
        dt += datetime.timedelta(days=step)
