import unittest
from threading import Lock, Thread
from typing import List, Callable, Set, Optional

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._data_management import DataLoaderManager
from cookiemonster.processor._models import Rule
from cookiemonster.processor._rules_management import RulesManager
from cookiemonster.processor.processing import ProcessorManager, Processor, RuleProcessingQueue


class BasicProcessor(Processor):
    """
    Simple processor for a single file update.
    """
    def process(self, cookie: Cookie, rules: Set[Rule],
                on_complete: Callable[[bool, Optional[Set[Notification]]], None]):
        rule_processing_queue = RuleProcessingQueue(rules)

        notifications = set()
        terminate = False

        while not terminate and rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_unprocessed()
            if rule.matching_criteria(cookie):
                rule_action = rule.action_generator(cookie)
                notifications = notifications.union(rule_action.notifications)
                terminate = rule_action.terminate_processing
            rule_processing_queue.mark_as_processed(rule)

        at_least_one_rule_matched = terminate or len(notifications) > 0
        on_complete(at_least_one_rule_matched, notifications)


class BasicProcessorManager(ProcessorManager):
    """
    Simple implementation of managing the continuous processing of new data.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_manager: RulesManager,
                 data_loader_manager: DataLoaderManager, notifier: Notifier):
        """
        Default constructor.
        :param number_of_processors:
        :param cookie_jar:
        :param rules_manager:
        :param data_loader_manager:
        :param notifier:
        :return:
        """
        self._cookie_jar = cookie_jar
        self._rules_manager = rules_manager
        self._notifier = notifier
        self._data_loader_manager = data_loader_manager

        self._idle_processors = set()
        self._busy_processors = set()
        self._lists_lock = Lock()

        for _ in range(number_of_processors):
            processor = BasicProcessor()
            self._idle_processors.add(processor)

    def process_any_cookie_jobs(self):
        processor = self._claim_processor()

        if processor is not None:
            cookie = self._cookie_jar.get_next_for_processing()

            if cookie is not None:
                def on_complete(rules_matched: bool, notifications: List[Notification]):
                    self.on_cookie_processed(cookie, rules_matched, notifications)
                    self._release_processor(processor)
                    self.process_any_cookie_jobs()

                Thread(target=processor.process, args=(cookie, self._rules_manager.get_rules(), on_complete)).start()

                # Process more jobs if possible
                self.process_any_cookie_jobs()
            else:
                self._release_processor(processor)

    def on_cookie_processed(self, cookie: Cookie, rules_matched: bool, notifications: List[Notification]=()):
        if rules_matched:
            for notification in notifications:
                self._notifier.do(notification)
            self._cookie_jar.mark_as_complete(cookie.path)
        else:
            enrichment = self._data_loader_manager.load_next(cookie)

            if enrichment is None:
                # FIXME: No guarantee that such a notification can be given
                self._notifier.do(Notification("unknown", cookie.path))
                self._cookie_jar.mark_as_complete(cookie.path)
            else:
                self._cookie_jar.enrich_metadata(cookie.path, enrichment)
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


if __name__ == "__main__":
    unittest.main()
