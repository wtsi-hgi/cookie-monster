import copy
from abc import ABCMeta, abstractmethod
from typing import Callable, Set, Optional

from multiprocessing import Lock

from cookiemonster.common.models import Notification, CookieProcessState
from cookiemonster.processor._models import Rule


class Processor(metaclass=ABCMeta):
    """
    Processor for a single file update.
    """
    @abstractmethod
    def process(self, job: CookieProcessState, rules: Set[Rule],
                on_complete: Callable[[bool, Optional[Set[Notification]]], None]):
        """
        Processes the given file update.
        :param job: the job that is to be processed
        :param rules: the processor to use when processing the job
        :param on_complete: called when the processing has completed. First argument indicates if at least one rule was
        matched and the second is a set of notifications that were generated.
        """
        pass


class RuleProcessingQueue:
    """
    A queue of processor that are to be processed.

    Thread-safe.
    """
    def __init__(self, rules: Set[Rule]):
        """
        Default constructor.
        :param rules: the processor to be processed
        """
        self._not_processed = []
        self._processed = []
        self._lists_lock = Lock()

        for rule in rules:
            self._not_processed.append(rule)

    def has_unprocessed_rules(self) -> bool:
        """
        Returns whether or not there exists processor that have not been processed.
        :return: whether there are processor that have not been processed
        """
        return len(self._not_processed) > 0

    def get_all(self) -> Set[Rule]:
        """
        Gets all of the processor.
        :return: all of the processor
        """
        return copy.deepcopy(set(self._not_processed + self._processed))

    def get_next_unprocessed(self) -> Optional[Rule]:
        """
        Gets the next rule that should be processed.
        :return: the next rule to be processed, else `None` if no more processor to process
        """
        if not self.has_unprocessed_rules():
            return None
        return copy.deepcopy(self._not_processed[-1])

    def mark_as_processed(self, rule: Rule):
        """
        Marks the given rule as processed. Will raise a `ValueError` if the rule has already been marked as processed.
        :param rule: the rule to mark as processed
        """
        if rule not in self._not_processed:
            assert rule in self._processed
            raise ValueError("Rule has already been marked as processed: %s" % rule)
        assert rule not in self._processed
        self._lists_lock.acquire()
        self._not_processed.remove(rule)
        self._processed.append(rule)
        self._lists_lock.release()

    def reset_all_marked_as_processed(self):
        """
        Resets all processor previously marked as processed.
        """
        while len(self._processed) != 0:
            self._lists_lock.acquire()
            rule = self._processed.pop()
            assert rule not in self._not_processed
            self._not_processed.append(rule)
            self._lists_lock.release()
