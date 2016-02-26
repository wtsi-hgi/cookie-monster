import logging
from datetime import timedelta
from threading import Semaphore, Timer
from typing import Optional

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar import BiscuitTin


class _RateLimitedSemaphore(Semaphore):
    """
    Semaphore that takes a second to release.
    """
    def release(self):
        Timer(1.0, super().release).start()


class RateLimitedBiscuitTin(BiscuitTin):
    """
    Subclass of `BiscuitTin` that limits the rate at which requests can be made to the database.
    """
    def __init__(self, max_requests_per_second: int, db_host: str, db_name: str,
                 notify_interval: timedelta=timedelta(minutes=10)):
        super().__init__(db_host, db_name, notify_interval)
        self._request_semaphore = _RateLimitedSemaphore(max_requests_per_second)

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        with self._request_semaphore:
            super().enrich_cookie(path, enrichment)

    def mark_as_failed(self, path: str, requeue_delay: Optional[timedelta]=None):
        with self._request_semaphore:
            super().mark_as_failed(path, requeue_delay=requeue_delay)

    def mark_as_complete(self, path: str):
        with self._request_semaphore:
            super().mark_as_complete(path)

    def mark_for_processing(self, path: str):
        with self._request_semaphore:
            super().mark_for_processing(path)

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._request_semaphore:
            return super().get_next_for_processing()

    def queue_length(self) -> int:
        with self._request_semaphore:
            return super().queue_length()
