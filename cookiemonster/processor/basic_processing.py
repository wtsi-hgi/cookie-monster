import copy
import logging
from threading import Lock, Thread
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
    Simple processor for a single cookie.
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
    Simple manager for the continuous processing of new data.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_source: DataSource[Rule],
                 enrichment_loader_source: DataSource[EnrichmentLoader],
                 notification_receivers_source: DataSource[NotificationReceiver]):
        """
        Constructor.
        :param number_of_processors: the maximum number of processors to use. Must be at least 1
        :param cookie_jar: the cookie jar to get updates from
        :param rules_source: the source of the rules
        :param enrichment_loader_source: the source of enrichment loaders
        :param notification_receivers_source: the source of notification receivers
        """
        if number_of_processors < 1:
            raise ValueError("Must be instantiated with at least one processor, not %d" % number_of_processors)

        self._cookie_jar = cookie_jar
        self._rules_source = rules_source
        self._notification_receivers_source = notification_receivers_source
        self._enrichment_loaders_source = enrichment_loader_source
        self._enrichment_manager = EnrichmentManager(self._enrichment_loaders_source)

        self._idle_processors = set()
        self._busy_processors = set()
        self._lists_lock = Lock()

        for _ in range(number_of_processors):
            processor = BasicProcessor()
            self._idle_processors.add(processor)

    def process_any_cookies(self):
        processor = self._claim_processor()

        if processor is not None:
            cookie = self._cookie_jar.get_next_for_processing()

            if cookie is not None:
                def on_complete(rules_matched: bool, notifications: List[Notification]):
                    self.on_cookie_processed(cookie, rules_matched, notifications)
                    self._release_processor(processor)
                    # One last task before the thread that ran the processor can end...
                    self.process_any_cookies()

                logging.info(
                    "Starting processor \"%s\" for cookie with path \"%s\". %d free processors remaining, %d cookies "
                    "queued for processing" % (id(processor), cookie.path, len(self._idle_processors),
                                               self._cookie_jar.queue_length()))
                Thread(target=processor.process, args=(cookie, self._rules_source.get_all(), on_complete)).start()
            else:
                self._release_processor(processor)
                logging.debug("Triggered to process cookies - no cookies to process. %d free processors"
                              % len(self._idle_processors))
        else:
            logging.debug("Triggered to process cookies but no free processors")

    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: Iterable[Notification]=()):
        for notification in notifications:
            self._notify_notification_receivers(notification)

        if stop_processing:
            logging.info("Stopping processing of cookie with path \"%s\"" % cookie.path)
            self._cookie_jar.mark_as_complete(cookie.path)
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
                self._cookie_jar.mark_as_complete(cookie.path)
            else:
                logging.info("Applying enrichment from source \"%s\" to cookie with path \"%s\""
                             % (enrichment.source, cookie.path))
                self._cookie_jar.enrich_cookie(cookie.path, enrichment)
                # Enrichment method sets cookie for processing when enriched so no need to repeat that

    def _claim_processor(self) -> Optional[Processor]:
        """
        Claims a processor.

        Thread-safe.
        :return: the claimed processor, else `None` if none were available
        """
        if len(self._idle_processors) == 0:
            return None
        with self._lists_lock:
            processor = self._idle_processors.pop()
            self._busy_processors.add(processor)
        return processor

    def _release_processor(self, processor: Processor):
        """
        Releases a processor.

        Thread-safe.
        :param processor: the processor that is currently in use and should be released
        """
        assert processor in self._busy_processors
        assert processor not in self._idle_processors
        with self._lists_lock:
            self._busy_processors.remove(processor)
            self._idle_processors.add(processor)

    def _notify_notification_receivers(self, notification: Notification):
        """
        Notifies the notification receivers of the given notification.

        Notification receivers are ran concurrently in separate threads.
        :param notification: the notification to give to all notification receivers
        """
        notification_receivers = self._notification_receivers_source.get_all()
        logging.info("Notifying %d notification receiver(s) of notification about \"%s\""
                     % (len(notification_receivers), notification.about))

        for notification_receiver in notification_receivers:
            Thread(target=notification_receiver.receive, args=(notification, )).start()
