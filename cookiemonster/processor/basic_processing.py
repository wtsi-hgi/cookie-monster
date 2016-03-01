import copy
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Callable, Optional, Sequence, Iterable

from hgicommon.data_source import DataSource

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifications.notification_receiver import NotificationReceiver
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._rules import RuleQueue
from cookiemonster.processor.models import Rule, EnrichmentLoader
from cookiemonster.processor.processing import ProcessorManager, Processor, ABOUT_NO_RULES_MATCH


class BasicProcessor(Processor):
    """
    Simple processor for a single Cookie.
    """
    def process(self, cookie: Cookie, rules: Sequence[Rule],
                on_complete: Callable[[bool, Optional[Iterable[Notification]]], None]):
        logging.info("Processor \"%s\" processing cookie with path \"%s\", which has %d enrichment(s). Using %d rule(s)"
                     % (id(self), cookie.path, len(cookie.enrichments), len(rules)))
        rule_queue = RuleQueue(rules)

        notifications = []
        terminate = False

        while not terminate and rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            isolated_cookie = copy.deepcopy(cookie)
            try:
                if rule.matches(isolated_cookie):
                    rule_action = rule.generate_action(isolated_cookie)
                    notifications += rule_action.notifications
                    terminate = rule_action.terminate_processing
            except Exception as e:
                logging.error("Error applying rule; Rule: %s; Error: %s" % (e, rule))
            rule_queue.mark_as_applied(rule)

        logging.info("Completed processing cookie with path \"%s\". Notifying %d external processes. "
                     "Stopping processing of cookie: %s" % (cookie.path, len(notifications), terminate))
        on_complete(terminate, notifications)


class BasicProcessorManager(ProcessorManager):
    """
    Simple manager for the continuous processing of enriched Cookies.
    """
    def __init__(self, max_cookies_to_process_simultaneously: int, cookie_jar: CookieJar,
                 rules_source: DataSource[Rule], enrichment_loader_source: DataSource[EnrichmentLoader],
                 notification_receivers_source: DataSource[NotificationReceiver]):
        """
        Constructor.
        :param max_cookies_to_process_simultaneously: the maximum number of cookies to process simultaneously. Must be
        at least one.
        :param cookie_jar: the cookie jar to get updates from
        :param rules_source: the source of the rules
        :param enrichment_loader_source: the source of enrichment loaders
        :param notification_receivers_source: the source of notification receivers
        """
        if max_cookies_to_process_simultaneously < 1:
            raise ValueError("Must be able to process at least one Cookie at a time, not %d"
                             % max_cookies_to_process_simultaneously)

        self._cookie_jar = cookie_jar
        self._rules_source = rules_source
        self._notification_receivers_source = notification_receivers_source
        self._enrichment_loaders_source = enrichment_loader_source
        self._enrichment_manager = EnrichmentManager(self._enrichment_loaders_source)
        self._cookie_processing_thread_pool = ThreadPoolExecutor(max_workers=max_cookies_to_process_simultaneously)

    def process_any_cookies(self):
        logging.info("Prompted to process any unprocessed cookies. (%s)" % self.get_status_string())
        self._cookie_processing_thread_pool.submit(self._process_any_cookies)

    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: Iterable[Notification]=()):
        # Broadcast notifications
        for notification in notifications:
            self._notify_notification_receivers(notification)

        # Relinquish claim on Cookie
        self._cookie_jar.mark_as_complete(cookie.path)

        if stop_processing:
            logging.info("Stopping processing of cookie with path \"%s\"" % cookie.path)
        else:
            logging.info(
                    "Checking if any of the %d enrichment loader(s) can load enrichment for cookie with path \"%s\""
                    % (len(self._enrichment_loaders_source.get_all()), cookie.path))
            enrichment = self._enrichment_manager.next_enrichment(cookie)

            if enrichment is None:
                logging.info("Cannot enrich cookie with path \"%s\" any further - marking as complete" % cookie.path)
                no_rules_match_notification = Notification(
                        ABOUT_NO_RULES_MATCH, cookie.path, BasicProcessorManager.__qualname__)
                self._notify_notification_receivers(no_rules_match_notification)
            else:
                logging.info("Applying enrichment from source \"%s\" to cookie with path \"%s\""
                             % (enrichment.source, cookie.path))
                self._cookie_jar.enrich_cookie(cookie.path, enrichment)
                # Enrichment method sets cookie for processing when enriched so no need to repeat that

    def get_status_string(self) -> str:
        """
        Gets string indicating the internal status of this manager.
        :return: human readable string
        """
        return "%d/%d cookies being processed simultaneously, %d cookies queued for processing, %d active threads" \
               % (len(self._cookie_processing_thread_pool._threads),
                  self._cookie_processing_thread_pool._max_workers,
                  self._cookie_jar.queue_length(), threading.active_count())

    def _process_any_cookies(self):
        """
        Process a cookie. Should be executed by a thead from the Cookie processing thread pool.
        """
        # Claim cookie
        cookie = self._cookie_jar.get_next_for_processing()

        if cookie is not None:
            def on_complete(rules_matched: bool, notifications: List[Notification]):
                self.on_cookie_processed(cookie, rules_matched, notifications)

            processor = BasicProcessor()
            logging.info("Processing cookie with path: %s. (%s)" % (cookie.path, self.get_status_string()))
            processor.process(cookie, self._rules_source.get_all(), on_complete)
        else:
            logging.info("Triggered to process cookies - no cookies to process. (%s)" % self.get_status_string())

    def _notify_notification_receivers(self, notification: Notification):
        """
        Notifies the notification receivers of the given notification.
        :param notification: the notification to give to all notification receivers
        """
        notification_receivers = self._notification_receivers_source.get_all()
        logging.info("Notifying %d notification receiver(s) of notification about \"%s\""
                     % (len(notification_receivers), notification.about))

        # TODO: This could be threaded
        for notification_receiver in notification_receivers:
            notification_receiver.receive(notification)
