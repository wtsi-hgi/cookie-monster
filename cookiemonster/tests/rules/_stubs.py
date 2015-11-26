from datetime import timedelta

from cookiemonster.common.models import CookieCrumbs, CookieProcessState, Notification
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier


class StubCookieJar(CookieJar):
    """
    Stub implementation of `CookieJar`.
    """
    def queue_length(self) -> int:
        pass

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        pass

    def mark_as_complete(self, path: str):
        pass

    def get_next_for_processing(self) -> CookieProcessState:
        pass

    def mark_as_reprocess(self, path: str):
        pass

    def enrich_metadata(self, path: str, metadata: CookieCrumbs):
        pass


class StubNotifier(Notifier):
    """
    Stub implementation of `Notifier`.
    """
    def do(self, notification: Notification):
        pass
