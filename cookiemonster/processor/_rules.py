import copy
from queue import PriorityQueue
from typing import Set, Container, Iterable, Optional

from multiprocessing import Lock

from cookiemonster.processor._models import Rule


class RuleProcessingQueue:
    """
    A queue of rules that are to be processed.

    Thread-safe.
    """
    def __init__(self, rules: Iterable[Rule]):
        """
        Default constructor.
        :param rules: the rules to be processed
        """
        self._rules = copy.copy(rules)
        self._not_processed = PriorityQueue()
        self._processed = []
        self._being_processed = []
        self._lists_lock = Lock()

        for rule in self._rules:
            self._not_processed.put(rule)

    def has_unprocessed_rules(self) -> bool:
        """
        Returns whether or not there exists rule that have not been processed.
        :return: whether there are rules that have not been processed
        """
        return not self._not_processed.empty()

    def get_next_to_process(self) -> Optional[Rule]:
        """
        Gets the next rule that should be processed. Marks as currently being processed.
        :return: the next rule to be processed, else `None` if no more to process
        """
        if not self.has_unprocessed_rules():
            return None
        self._lists_lock.acquire()
        rule = self._not_processed.get()
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

    def reset_processed(self):
        """
        Resets all rules previously marked as processed and those marked as being processed.
        """
        self._lists_lock.acquire()
        del self._being_processed[:]
        del self._processed[:]

        while not self._not_processed.empty():
            self._not_processed.get()
        for rule in self._rules:
            self._not_processed.put(rule)
        self._lists_lock.release()


class InFileRulesMonitor:
    """
    Whether changes to the processor, defined by changes in the files in the directory.
    """
    def __init__(self, directory_location: str, rules: Container[Rule]):
        """
        Default constructor.
        :param directory_location: the location of the processor
        :param rules: the processor manager to update about changes in the processor
        """
        raise NotImplementedError()

    def is_monitoring(self) -> bool:
        """
        Whether this monitor is monitoring.
        :return: state of the monitor
        """
        raise NotImplementedError()

    def start(self):
        """
        Starts monitoring processor in the directory.
        """
        raise NotImplementedError()

    def stop(self):
        """
        Stops monitoring processor in the directory.
        """
        raise NotImplementedError()
