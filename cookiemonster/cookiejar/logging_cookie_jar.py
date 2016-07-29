"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from typing import Callable, Dict, Optional

from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import Logger
from cookiemonster.logging.injector import LoggingContext, RuntimeLogging, LoggingMapper


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


class _CookieJarRuntimeLogging(RuntimeLogging):
    def get_measure(self, context:LoggingContext) -> str:
        fn_name = context.name
        return MEASUREMENT_QUERY_TIME[fn_name]

    def get_metadata(self, context:LoggingContext) -> Optional[Dict]:
        return None


def _create_mapping(logger:Logger) -> LoggingMapper:
    """
    Create the logging mapping from CookieJar abstract methods to the
    Cookie Jar runtime logging function

    @param   logger  Where to log query times to
    @return  Logging mapping
    """
    mapping = LoggingMapper(logger)
    mapping.map_logging_to_abstract_methods(CookieJar, _CookieJarRuntimeLogging)
    return mapping


def add_cookie_jar_logging(cookie_jar: CookieJar, logger: Logger):
    """
    Modifies the given `CookieJar` instance so that the time taken to
    complete calls to its functions is logged

    :param cookie_jar: the `CookieJar` to add logging to
    :param logger: where to log query times to
    """
    mapping = _create_mapping(logger)
    mapping.inject_logging(cookie_jar)


def logging_cookie_jar(logger:Logger) -> Callable[[type], type]:
    """
    Parametrised class decorator that applies the runtime logging
    mapping to the class' abstract methods

    @param   logger  Where to log query times to
    @return  Class decorator
    """
    mapping = _create_mapping(logger)

    def decorator(cookie_jar_cls:type) -> type:
        """ Class decorator """
        return mapping.inject_logging(cookie_jar_cls)

    return decorator
