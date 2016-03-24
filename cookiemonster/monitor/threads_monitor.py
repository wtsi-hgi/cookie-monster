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
import threading

from cookiemonster.monitor.monitor import Monitor

MEASURED_NUMBER_OF_THREADS = "number_of_threads"


class ThreadsMonitor(Monitor):
    """
    Monitors the total number of Python threads that are running.
    """
    def do_log_record(self):
        number_of_threads = threading.active_count()
        self._logger.record(MEASURED_NUMBER_OF_THREADS, number_of_threads)
