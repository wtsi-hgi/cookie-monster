from abc import ABCMeta
from abc import abstractmethod
from threading import Lock
from typing import List, Optional, Callable, Iterable

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.processor._models import Rule


class Processor(metaclass=ABCMeta):
    """
    Processor for a single file update.
    """
    @abstractmethod
    def process(self, cookie: Cookie, rules: Iterable[Rule],
                on_complete: Callable[[bool, Optional[List[Notification]]], None]):
        """
        Processes the given cookie.
        :param cookie: the cookie that is to be processed
        :param rules: the processor to use when processing the cookie
        :param on_complete: called when the processing has completed. First argument indicates if at least one rule was
        matched and the second is a set of notifications that were generated.
        """
        pass


class ProcessorManager(metaclass=ABCMeta):
    """
    Manages the continuous processing of file updates.
    """
    @abstractmethod
    def process_any_cookies(self):
        """
        Check for new cookie jobs that are to be processed and process them if they are available.
        """
        pass

    @abstractmethod
    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: List[Notification]=()):
        """
        Called when processing of a cookie has been completed
        :param cookie: the cookie that has been processed
        :param stop_processing: whether rule indicates that we should stop processing the given cookie
        :param notifications: list of external processes that are to be notified. List should only be givne if processor
        were matched
        """
        pass


class RuleProcessingQueue:
    """
    A queue of processor that are to be processed.

    Thread-safe.
    """
    def __init__(self, rules: Iterable[Rule]):
        """
        Default constructor.
        :param rules: the rules to be processed
        """
        self._not_processed = []
        self._processed = []
        self._being_processed = []
        self._lists_lock = Lock()

        for rule in rules:
            if rule in self._not_processed:
                raise ValueError("Cannot add duplicate rule %s" % rule)
            self._not_processed.append(rule)

    def has_unprocessed_rules(self) -> bool:
        """
        Returns whether or not there exists rule that have not been processed.
        :return: whether there are rules that have not been processed
        """
        return len(self._not_processed) > 0

    def get_all(self) -> Iterable[Rule]:
        """
        Gets all of the rules.
        :return: all of the rules
        """
        return self._not_processed + self._processed + self._being_processed

    def get_next_to_process(self) -> Optional[Rule]:
        """
        Gets the next rule that should be processed. Marks as currently being processed.
        :return: the next rule to be processed, else `None` if no more to process
        """
        if not self.has_unprocessed_rules():
            return None
        self._lists_lock.acquire()
        rule = self._not_processed.pop()
        assert rule not in self._processed
        assert rule not in self._being_processed
        self._being_processed.append(rule)
        self._lists_lock.release()
        return rule

    def mark_as_processed(self, rule: Rule):
        """
        Marks the given rule as processed. Will raise a `ValueError` if the rule has already been marked as processed or
        if the rule is not marked as being processed (i.e. acquired via `get_next_to_process`).
        :param rule: the rule to mark as processed
        """
        if rule in self._processed:
            raise ValueError("Rule has already been marked as processed: %s" % rule)
        if rule not in self._being_processed:
            raise ValueError("Rule not marked as being processed: %s" % rule)
        self._lists_lock.acquire()
        self._being_processed.remove(rule)
        self._processed.append(rule)
        self._lists_lock.release()

    def reset_all_marked_as_processed(self):
        """
        Resets all rules previously marked as processed and those marked as being processed.
        """
        def reset(to_reset: List[Rule]):
            while len(to_reset) != 0:
                rule = to_reset.pop()
                assert rule not in self._not_processed
                self._not_processed.append(rule)

        number_of_rules = len(self.get_all())
        self._lists_lock.acquire()
        reset(self._being_processed)
        reset(self._processed)
        assert len(self._being_processed) == 0
        assert len(self._processed) == 0
        assert len(self._not_processed) == number_of_rules
        self._lists_lock.release()
