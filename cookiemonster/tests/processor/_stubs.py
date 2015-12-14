from datetime import timedelta
from typing import Optional

from cookiemonster.common.models import Notification, Enrichment, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier


class StubCookieJar(CookieJar):
    """
    Stub implementation of `CookieJar`.
    """
    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        pass

    def mark_as_complete(self, path: str):
        pass

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        pass

    def mark_as_reprocess(self, path: str):
        pass

    def queue_length(self) -> int:
        pass

    def get_next_for_processing(self) -> Optional[Cookie]:
        pass


class StubNotifier(Notifier):
    """
    Stub implementation of `Notifier`.
    """
    def do(self, notification: Notification):
        pass
