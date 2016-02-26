import logging
from datetime import timedelta
from threading import Semaphore, Timer
from typing import Optional

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar import BiscuitTin


class RateLimitedBiscuitTin(BiscuitTin):
    """
    Subclass of `BiscuitTin` that limits the rate at which requests can be made to the database.
    """
    def __init__(self, max_requests_per_second: int, db_host: str, db_name: str,
                 notify_interval: timedelta=timedelta(minutes=10)):
        super().__init__(db_host, db_name, notify_interval)
        self._request_semaphore = Semaphore(max_requests_per_second)

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        self._request_semaphore.acquire()
        super().enrich_cookie(path, enrichment)
        self._delay_release_request_semaphore()

    def mark_as_failed(self, path: str, requeue_delay: Optional[timedelta]=None):
        self._request_semaphore.acquire()
        super().mark_as_failed(path, requeue_delay=requeue_delay)
        self._delay_release_request_semaphore()

    def mark_as_complete(self, path: str):
        self._request_semaphore.acquire()
        super().mark_as_complete(path)
        self._delay_release_request_semaphore()

    def mark_for_processing(self, path: str):
        self._request_semaphore.acquire()
        super().mark_for_processing(path)
        self._delay_release_request_semaphore()

    def get_next_for_processing(self) -> Optional[Cookie]:
        self._request_semaphore.acquire()
        return_value = super().get_next_for_processing()
        self._delay_release_request_semaphore()
        return return_value

    def queue_length(self) -> int:
        self._request_semaphore.acquire()
        return_value = super().queue_length()
        self._delay_release_request_semaphore()
        return return_value

    def _delay_release_request_semaphore(self):
        """
        Non-blocking delayed release of the request semaphore.
        """
        Timer(1.0, self._request_semaphore.release).start()
