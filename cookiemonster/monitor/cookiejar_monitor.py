"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from datetime import timedelta

from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import Logger
from cookiemonster.monitor.monitor import Monitor

MEASURED_COOKIE_JAR_STATUS = "cookie_jar_status"
MEASURED_COOKIE_JAR_TO_PROCESS = "to_process"


class CookieJarMonitor(Monitor):
    """
    Monitors the status of a `CookieJar`.
    """
    def __init__(self, logger: Logger, period: timedelta, cookie_jar: CookieJar):
        """
        Constructor.
        :param logger:
        :param period:
        :param cookie_jar:
        """
        super().__init__(logger, period)
        self._cookie_jar = cookie_jar

    def do_log_record(self):
        cookies_to_process = self._cookie_jar.queue_length()
        self._logger.record(
            MEASURED_COOKIE_JAR_STATUS,
            {
                MEASURED_COOKIE_JAR_TO_PROCESS: cookies_to_process
            }
        )
