import time
from functools import wraps
from typing import Callable

from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import Logger

MEASUREMENT_QUERY_TIME = {
    CookieJar.fetch_cookie.__name__: "fetch_cookie_time",
    CookieJar.delete_cookie.__name__: "delete_cookie_time",
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


def add_cookie_jar_logging(cookie_jar: CookieJar, logger: Logger):
    """
    Modifies the given `CookieJar` instance so that the time taken to complete calls to its functions is logged.
    :param cookie_jar: the `CookieJar` to add logging to
    :param logger: where to log query times to
    """
    for method_name in CookieJar.__abstractmethods__:
        setattr(cookie_jar, method_name, _timer_wrap(method_name, getattr(cookie_jar, method_name), logger))


class LoggingCookieJar(CookieJar):
    """
    `CookieJar` implementation that logs the amount of time taken to complete `CookieJar` function calls.
    """


def logging_cookie_jar(cookie_jar_cls: type) -> type:
    """
    Creates a decorate that uses an instances of the given `CookieJar` class as the decorated component.
    :param cookie_jar_cls: the class to decorate
    :return: the decorated class
    """
    def decorator_init(cookie_jar: CookieJar, logger: Logger, *args, **kwargs):
        super(type(cookie_jar), cookie_jar).__init__(*args, **kwargs)
        add_cookie_jar_logging(cookie_jar, logger)

    return type(
        "%sLoggingCookieJar" % cookie_jar_cls,
        (cookie_jar_cls, LoggingCookieJar),
        {
            "__init__": decorator_init
        }
    )
