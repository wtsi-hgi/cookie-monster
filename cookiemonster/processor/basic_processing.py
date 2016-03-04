import copy
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Sequence, Iterable

import time
from hgicommon.data_source import DataSource

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifications.notification_receiver import NotificationReceiver
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._rules import RuleQueue
from cookiemonster.processor.models import Rule, EnrichmentLoader, RuleAction
from cookiemonster.processor.processing import ProcessorManager, Processor, ABOUT_NO_RULES_MATCH


class BasicProcessor(Processor):
    """
    Simple processor for a single Cookie.
    """
    def __init__(self, cookie_jar: CookieJar, rules: Sequence[Rule], enrichment_loaders: Sequence[EnrichmentLoader],
                 notification_receivers: Sequence[NotificationReceiver]):
        """
        Constructor.
        :param cookie_jar: the cookie jar to use
        :param rules: the rules to process the Cookie with
        :param enrichment_loaders: the enrichment loaders that may be able to enrich the Cookie
        :param notification_receivers: the recievers of notifications
        """
        self.cookie_jar = cookie_jar
        self.rules = rules
        self.notification_receivers = notification_receivers
        self.enrichment_loaders = enrichment_loaders

    def evaluate_rules_with_cookie(self, cookie: Cookie) -> Sequence[RuleAction]:
        rule_queue = RuleQueue(self.rules)

        rule_actions = []
        terminate = False

        while not terminate and rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            isolated_cookie = copy.deepcopy(cookie)
            try:
                if rule.matches(isolated_cookie):
                    rule_action = rule.generate_action(isolated_cookie)
                    rule_actions.append(rule_action)
                    terminate = rule_action.terminate_processing
            except Exception as e:
                logging.error("Error applying rule; Rule: %s; Error: %s" % (e, rule))
            rule_queue.mark_as_applied(rule)

        return rule_actions

    def execute_rule_actions(self, rule_actions: Iterable[RuleAction]):
        # Broadcast notifications
        for rule_action in rule_actions:
            for notification in rule_action.notifications:
                self._broadcast_notification(notification)

    def handle_cookie_enrichment(self, cookie: Cookie):
        logging.info("Checking if any of the %d enrichment loader(s) can load enrichment for cookie with path \"%s\""
                     % (len(self.enrichment_loaders), cookie.path))
        enrichment_manager = EnrichmentManager(self.enrichment_loaders)
        enrichment = enrichment_manager.next_enrichment(cookie)

        if enrichment is None:
            logging.info("Cannot enrich cookie with path \"%s\" any further - marking as complete" % cookie.path)
            no_rules_match_notification = Notification(ABOUT_NO_RULES_MATCH, cookie.path, BasicProcessor.__qualname__)
            self._broadcast_notification(no_rules_match_notification)
        else:
            logging.info("Applying enrichment from source \"%s\" to cookie with path \"%s\""
                         % (enrichment.source, cookie.path))
            self.cookie_jar.enrich_cookie(cookie.path, enrichment)
            # Enrichment method sets cookie for processing when enriched so no need to repeat that

    def _broadcast_notification(self, notification: Notification):
        """
        Notifies the notification receivers of the given notification.
        :param notification: the notification to give to all notification receivers
        """
        logging.info("Notifying %d notification receiver(s) of notification about \"%s\""
                     % (len(self.notification_receivers), notification.about))

        for notification_receiver in self.notification_receivers:
            notification_receiver.receive(notification)


class BasicProcessorManager(ProcessorManager):
    """
    Simple manager for the continuous processing of enriched Cookies.
    """
    def __init__(self, cookie_jar: CookieJar,
                 rules_source: DataSource[Rule], enrichment_loaders_source: DataSource[EnrichmentLoader],
                 notification_receivers_source: DataSource[NotificationReceiver], number_of_threads: int=16):
        """
        Constructor.
        :param cookie_jar: the cookie jar to get updates from
        :param rules_source: the source of the rules
        :param enrichment_loaders_source: the source of enrichment loaders
        :param notification_receivers_source: the source of notification receivers
        :param number_of_threads: the maximum number of threads to use
        """
        if number_of_threads < 1:
            raise ValueError("Must specific the use of at least one thread, not %d" % number_of_threads)

        self._cookie_jar = cookie_jar
        self._rules_source = rules_source
        self._notification_receivers_source = notification_receivers_source
        self._enrichment_loaders_source = enrichment_loaders_source
        self._cookie_processing_thread_pool = ThreadPoolExecutor(max_workers=number_of_threads)
        self._currently_processing_count = 0

    def process_any_cookies(self):
        logging.debug("Prompted to process any unprocessed cookies.")
        self._cookie_processing_thread_pool.submit(self._process_any_cookies)

    def get_status_string(self) -> str:
        """
        Gets a string indicating the internal status of this manager.
        :return: human readable string
        """
        return "%d/%d cookies being processed simultaneously, %d cookies queued for processing, %d active threads" \
               % (self._currently_processing_count, self._cookie_processing_thread_pool._max_workers,
                  self._cookie_jar.queue_length(), threading.active_count())

    def _process_any_cookies(self):
        """
        Processes any cookies, blocking whilst the Cookie is processed.
        """
        # Claim cookie
        cookie = self._cookie_jar.get_next_for_processing()

        if cookie is None:
            logging.info("Triggered to process cookies but none need processing. (%s)" % self.get_status_string())
        else:
            # Check if there is more Cookies that need to be processed
            self.process_any_cookies()

            logging.info("Processing cookie with path: \"%s\". (%s)" % (cookie.path, self.get_status_string()))
            started_at = time.monotonic()

            processor = BasicProcessor(self._cookie_jar, self._rules_source.get_all(),
                                       self._enrichment_loaders_source.get_all(),
                                       self._notification_receivers_source.get_all())
            try:
                # Process Cookie
                self._currently_processing_count += 1
                processor.process_cookie(cookie)

                # Relinquish claim on Cookie
                self._cookie_jar.mark_as_complete(cookie.path)

                total_time = time.monotonic() - started_at
                logging.info("Processed cookie with path \"%s\" in %f seconds (wall time)." % (cookie.path, total_time))
            except Exception as e:
                logging.error("Exception raised whilst processing cookie with path \"%s\": %s" % (cookie.path, e))

                # Relinquish claim on Cookie but state processing as failed
                self._cookie_jar.mark_as_failed(cookie.path)
            finally:
                self._currently_processing_count -= 1
