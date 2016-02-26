from datetime import timedelta
from threading import Semaphore, Timer
from typing import Optional

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar import BiscuitTin


class RateLimitedBiscuitTin(BiscuitTin):
    """
    Subclass to `BiscuitTin` that limits the rate at which requests can be made to the database.
    """
    def __init__(self, max_requests_per_second: int, db_host: str, db_name: str,
                 notify_interval: timedelta=timedelta(minutes=10)):
        super().__init__(db_host, db_name, notify_interval)
        self._request_semaphore = Semaphore(max_requests_per_second)

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        self._get_request_permission()
        super().enrich_cookie(path, enrichment)

    def mark_as_failed(self, path: str, requeue_delay: Optional[timedelta]=None):
        self._get_request_permission()
        self.mark_as_failed(path, requeue_delay=requeue_delay)

    def mark_as_complete(self, path: str):
        self._get_request_permission()
        self.mark_as_complete(path)

    def mark_for_processing(self, path: str):
        self._get_request_permission()
        self.mark_for_processing(path)

    def get_next_for_processing(self) -> Optional[Cookie]:
        self._get_request_permission()
        return self.get_next_for_processing()

    def queue_length(self) -> int:
        self._get_request_permission()
        return self.queue_length()

    def _get_request_permission(self):
        """
        Blocks until has permission to do a request.
        """
        self._request_semaphore.acquire()
        Timer(1.0, self._request_semaphore.release).start()
