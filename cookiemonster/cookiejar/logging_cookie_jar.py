import time
from functools import wraps
from typing import Callable

from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import Logger

MEASUREMENT_QUERY_TIME = {
    CookieJar.enrich_cookie.__name__: "enrich_cookie_time",
    CookieJar.mark_as_failed.__name__: "mark_as_failed_time",
    CookieJar.mark_as_complete.__name__: "mark_as_complete_time",
    CookieJar.mark_for_processing.__name__: "mark_for_processing",
    CookieJar.get_next_for_processing.__name__: "get_next_for_processing_time",
    CookieJar.queue_length.__name__: "queue_length_time"
}


def _timer_wrap(method_name: str, method: Callable, logger: Logger) -> Callable:
    """
    Wraps the given method such that the time taken to complete the call to it is timed and then logged.
    :param method_name: the name of the method that is to be wrapped
    :param method: the method that is to be wrapped
    :param logger: where to log timings to
    """
    @wraps(method)
    def wrapper(*args, **kwargs):
        started_at = time.monotonic()
        return_value = method(*args, **kwargs)
        duration = time.monotonic() - started_at
        logger.record(MEASUREMENT_QUERY_TIME[method_name], duration)
        return return_value
    return wrapper


def logging_cookie_jar(cookie_jar: CookieJar, logger: Logger) -> CookieJar:
    """
    CookieJar decorator that wraps a given `CookieJar` implementation and logs the time taken to complete calls to its
    functions.

    Modifies the given `CookieJar` instance.
    :param cookie_jar: the `CookieJar` to decorate
    :param logger: where to log query times to
    :return: decorated `CookieJar`
    """
    for method_name in CookieJar.__abstractmethods__:
        setattr(cookie_jar, method_name, _timer_wrap(method_name, getattr(cookie_jar, method_name), logger))

    return cookie_jar
