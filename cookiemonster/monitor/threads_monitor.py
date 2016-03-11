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
