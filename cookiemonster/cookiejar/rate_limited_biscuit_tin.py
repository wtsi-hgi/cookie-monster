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
    def __init__(self, max_requests_per_second: int, db_host: str, db_name: str):
        super().__init__(db_host, db_name, 1, timedelta(0))
        self._request_semaphore = _RateLimitedSemaphore(max_requests_per_second)

    def enrich_cookie(self, identifier: str, enrichment: Enrichment):
        with self._request_semaphore:
            super().enrich_cookie(identifier, enrichment)

    def mark_as_failed(self, identifier: str, requeue_delay: timedelta=timedelta(0)):
        with self._request_semaphore:
            super().mark_as_failed(identifier, requeue_delay=requeue_delay)

    def mark_as_complete(self, identifier: str):
        with self._request_semaphore:
            super().mark_as_complete(identifier)

    def mark_for_processing(self, identifier: str):
        with self._request_semaphore:
            super().mark_for_processing(identifier)

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._request_semaphore:
            return super().get_next_for_processing()

    def queue_length(self) -> int:
        with self._request_semaphore:
            return super().queue_length()
