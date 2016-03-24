"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

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
