import datetime

import pytz


def localise_to_utc(timestamp: datetime) -> datetime:
    """
    Localise the given timestamp to UTC.
    :param timestamp: the timestamp to localise
    :return: the timestamp localised to UTC
    """
    if timestamp.tzinfo is None:
        return pytz.utc.localize(timestamp)
    else:
        return timestamp.astimezone(pytz.utc)
