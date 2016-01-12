from datetime import timedelta
from typing import Optional, Callable

from cookiemonster.common.models import Notification, Enrichment, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifications.notification_receiver import NotificationReceiver


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

    def mark_for_processing(self, path: str):
        pass

    def queue_length(self) -> int:
        pass

    def get_next_for_processing(self) -> Optional[Cookie]:
        pass


class StubNotificationReceiver(NotificationReceiver):
    """
    Stub implementation of `NotificationReceiver`.
    """
    def __init__(self):
        super().__init__(lambda notification: None)

    def receive(self, notification: Notification):
        pass
