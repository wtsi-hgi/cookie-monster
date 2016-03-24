"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
from datetime import timedelta
from typing import Optional, Iterable, Sequence

from cookiemonster import RuleAction, NotificationReceiver
from cookiemonster.common.models import Notification, Enrichment, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifications.notification_receiver import NotificationReceiver
from cookiemonster.processor.processing import Processor


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


class StubProcessor(Processor):
    """
    Stub implementation of `Processor`.
    """
    def execute_rule_actions(self, rule_actions: Iterable[RuleAction]):
        pass

    def evaluate_rules_with_cookie(self, cookie: Cookie) -> Sequence[RuleAction]:
        pass

    def handle_cookie_enrichment(self, cookie: Cookie):
        pass
