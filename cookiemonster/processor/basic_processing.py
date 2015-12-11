from threading import Lock, Thread
from typing import List, Callable, Optional, Iterable, Container

from hgicommon.data_source import DataSource

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor.models import Rule
from cookiemonster.processor._rules import RuleProcessingQueue, RulesSource
from cookiemonster.processor.processing import ProcessorManager, Processor


class BasicProcessor(Processor):
    """
    Simple processor for a single file update.
    """
    def process(self, cookie: Cookie, rules: Iterable[Rule],
                on_complete: Callable[[bool, Optional[List[Notification]]], None]):
        rule_processing_queue = RuleProcessingQueue(rules)

        notifications = []
        terminate = False

        while not terminate and rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_to_process()
            if rule.matching_criteria(cookie):
                rule_action = rule.action_generator(cookie)
                notifications += rule_action.notifications
                terminate = rule_action.terminate_processing
            rule_processing_queue.mark_as_processed(rule)

        on_complete(terminate, notifications)


class BasicProcessorManager(ProcessorManager):
    """
    Simple manager for the continuous processing of new data.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_source: DataSource[Rule],
                 enrichment_manager: EnrichmentManager, notifier: Notifier):
        """
        Default constructor.
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

                Thread(target=processor.process, args=(cookie, self._rules_source.get_all(), on_complete)).start()

                # Process more jobs if possible
                self.process_any_cookies()
            else:
                self._release_processor(processor)

    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: List[Notification]=()):
        for notification in notifications:
            self._notifier.do(notification)

        if stop_processing:
            self._cookie_jar.mark_as_complete(cookie.path)
        else:
            enrichment = self._enrichment_manager.next_enrichment(cookie)

            if enrichment is None:
                # FIXME: No guarantee that such a notification can be given
                self._notifier.do(Notification("unknown", cookie.path))
                self._cookie_jar.mark_as_complete(cookie.path)
            else:
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
        self._lists_lock.acquire()
        processor = self._idle_processors.pop()
        self._busy_processors.add(processor)
        self._lists_lock.release()
        return processor

    def _release_processor(self, processor: Processor):
        """
        Releases a processor.

        Thread-safe.
        :param processor: the processor that is currently in use and should be released
        """
        assert processor in self._busy_processors
        assert processor not in self._idle_processors
        self._lists_lock.acquire()
        self._busy_processors.remove(processor)
        self._idle_processors.add(processor)
        self._lists_lock.release()
