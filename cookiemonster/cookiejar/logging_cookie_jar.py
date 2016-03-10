from typing import Optional, Callable, Any

from datetime import timedelta
import time

from hgicommon.models import Model

from cookiemonster import Cookie, Enrichment
from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import Logger


MEASUREMENT_ENRICH_COOKIE_TIME = "enrich_cookie_time"
MEASUREMENT_MARK_AS_FAILED_TIME = "mark_as_failed_time"
MEASUREMENT_MARK_AS_COMPLETE_TIME = "mark_as_complete_time"
MEASUREMENT_MARK_FOR_PROCESSING_TIME = "mark_for_processing"
MEASUREMENT_GET_NEXT_FOR_PROCESSING_TIME = "get_next_for_processing_time"
MEASUREMENT_QUEUE_LENGTH_TIME = "queue_length_time"

_DurationInSeconds = int


class _TimedFunctionCallResult(Model):
    """
    Model of a timed function call result.
    """
    def __init__(self, duration: _DurationInSeconds, return_value: Any):
        self.duration = duration
        self.return_value = return_value


def _time_method_call(fn: Callable, *function_args, **function_kwargs) -> _TimedFunctionCallResult:
    """
    Times the given function call.
    :param fn: the function to time
    :param function_args: the positional arguments to give to the function that is been timed
    :param function_kwargs: the named arguments to give to the function that is been timed
    :return: the return of the function along with the time taken to complete the call
    """
    # TODO: Look if there is a library that does this when have Internet...
    started_at = time.monotonic()
    return_value = fn(*function_args, **function_kwargs)
    duration = time.monotonic() - started_at
    return _TimedFunctionCallResult(duration, return_value)


class LoggingCookieJar(CookieJar):
    """
    CookieJar decorator that wraps a given `CookieJar` implementation and logs the time taken to complete calls to its
    functions.
    """
    def __init__(self, cookie_jar: CookieJar, logger: Logger):
        """
        Constructor.
        :param cookie_jar: the `CookieJar` implementation to log method calls for
        :param logger: the log recorder
        """
        super().__init__()
        self._super_properties = dir(super())
        self._composite_cookie_jar = cookie_jar
        self._logger = logger

    def __getattribute__(self, attribute: str):
        # This override ensures methods/properties of composite are used unless defined in the adapter. Does not apply
        # to magic methods though
        local_property = attribute in object.__getattribute__(self, "__dict__").keys()
        super_property = attribute in object.__getattribute__(self, "_super_properties")
        magic_property = attribute.startswith("__")

        if attribute in vars(LoggingCookieJar) or (magic_property or local_property and not super_property):
            return object.__getattribute__(self, attribute)
        else:
            return type(self._composite_cookie_jar).__getattribute__(self._composite_cookie_jar, attribute)

    def enrich_cookie(self, identifier: str, enrichment: Enrichment):
        result = _time_method_call(self._composite_cookie_jar.enrich_cookie, identifier, enrichment)
        self._logger.record(MEASUREMENT_ENRICH_COOKIE_TIME, result.duration)

    def mark_as_failed(self, identifier: str, requeue_delay: timedelta):
        result = _time_method_call(self._composite_cookie_jar.mark_as_failed, identifier, requeue_delay)
        self._logger.record(MEASUREMENT_MARK_AS_FAILED_TIME, result.duration)

    def mark_as_complete(self, identifier: str):
        result = _time_method_call(self._composite_cookie_jar.mark_as_complete, identifier)
        self._logger.record(MEASUREMENT_MARK_AS_COMPLETE_TIME, result.duration)

    def mark_for_processing(self, identifier: str):
        result = _time_method_call(self._composite_cookie_jar.mark_for_processing, identifier)
        self._logger.record(MEASUREMENT_MARK_FOR_PROCESSING_TIME, result.duration)

    def get_next_for_processing(self) -> Optional[Cookie]:
        result = _time_method_call(self._composite_cookie_jar.get_next_for_processing)
        self._logger.record(MEASUREMENT_GET_NEXT_FOR_PROCESSING_TIME, result.duration)
        return result.return_value

    def queue_length(self) -> int:
        result = _time_method_call(self._composite_cookie_jar.queue_length)
        self._logger.record(MEASUREMENT_QUEUE_LENGTH_TIME, result.duration)
        return result.return_value
