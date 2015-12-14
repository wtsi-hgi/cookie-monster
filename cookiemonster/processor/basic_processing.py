import logging
from threading import Lock, Thread
from typing import List, Callable, Optional, Sequence

from hgicommon.data_source import DataSource

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._rules import RuleQueue
from cookiemonster.processor.models import Rule
from cookiemonster.processor.processing import ProcessorManager, Processor


class BasicProcessor(Processor):
    """
    Simple processor for a single file update.
    """
    def process(self, cookie: Cookie, rules: Sequence[Rule],
                on_complete: Callable[[bool, Optional[List[Notification]]], None]):
        logging.info("Processing cookie with path \"%s\", which has %d enrichment(s). Using %d rule(s)"
                     % (cookie.path, len(cookie.enrichments), len(rules)))
        rule_queue = RuleQueue(rules)

        notifications = []
        terminate = False

        while not terminate and rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            if rule.matching_criteria(cookie):
                rule_action = rule.action_generator(cookie)
                notifications += rule_action.notifications
                terminate = rule_action.terminate_processing
            rule_queue.mark_as_applied(rule)

        logging.info("Completed processing cookie with path \"%s\". Notifying %d external processes. "
                     "Stopping processing of cookie: %s" % (cookie.path, len(notifications), terminate))
        on_complete(terminate, notifications)


class BasicProcessorManager(ProcessorManager):
    """
    Simple manager for the continuous processing of new data.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_source: DataSource[Rule],
                 enrichment_manager: EnrichmentManager, notifier: Notifier):
        """
        Constructor.
        :param number_of_processors: the maximum number of processors to use
        :param cookie_jar: the cookie jar to get updates from
        :param rules_source: the source of the rules
        :param enrichment_manager: the manager to use when loading more information about a cookie
        :param notifier: the notifier
        """
        self._cookie_jar = cookie_jar
        self._rules_source = rules_source
        self._notifier = notifier
        self._enrichment_manager = enrichment_manager

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

                logging.debug("Starting processor for cookie with path \"%s\". %d free processors remaining"
                              % (cookie.path, len(self._idle_processors)))
                Thread(target=processor.process, args=(cookie, self._rules_source.get_all(), on_complete)).start()
            else:
                self._release_processor(processor)
                logging.debug("Triggered to process cookies - no cookies to process")
        else:
            logging.debug("Triggered to process cookies but no free processors")

    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: List[Notification]=()):
        for notification in notifications:
            logging.info("Notifying \"%s\" as a result of processing cookie with path \"%s\""
                          % (notification.external_process_name, cookie.path))
            self._notifier.do(notification)

        if stop_processing:
            logging.info("Stopping processing of cookie with path \"%s\"" % cookie.path)
            self._cookie_jar.mark_as_complete(cookie.path)
        else:
            enrichment = self._enrichment_manager.next_enrichment(cookie)

            if enrichment is None:
                logging.info("Cannot enrich cookie with path \"%s\" any further" % cookie.path)
                # FIXME: No guarantee that such a notification can be given
                self._notifier.do(Notification("unknown", cookie.path))
                self._cookie_jar.mark_as_complete(cookie.path)
            else:
                logging.info("Appliyng enrichment from source \"%s\" to cookie with path \"%s\""
                              % (cookie.path, enrichment.source))
                self._cookie_jar.enrich_cookie(cookie.path, enrichment)
                self._cookie_jar.mark_as_reprocess(cookie.path)

    def _claim_processor(self) -> Optional[Processor]:
        """
        Claims a processor.

        Thread-safe.
        :return: the claimed processor, else `None` if no were available
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
