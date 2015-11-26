from typing import Any, List

from cookiemonster.common.models import FileUpdate, Notification
from cookiemonster.cookiejar import CookieJar
from cookiemonster.rules.manager import ProcessorManager
from cookiemonster.rules._rules import RulesManager
from cookiemonster.rules._simple_processor import SimpleProcessor
from cookiemonster.rules.processor import Processor


class SimpleProcessorManager(ProcessorManager):
    """
    Basic implementation to of managing the continuous processing of file updates.
    """
    def __init__(self, number_of_processors: int, data_manager: CookieJar, rules_manager: RulesManager, notifier: Any):
        """
        Default constructor.
        :param number_of_processors:
        :param data_manager:
        :param rules_manager:
        :param notifier:
        """
        self._data_manager = data_manager
        self._rules_manager = rules_manager
        self._notifier = notifier

        self._idle_processors = []
        self._busy_processors = []

        for i in range(number_of_processors):
            processor = Processor()
            self._idle_processors.append(processor)

    def on_information(self):
        if self._are_free_processors():
            work = self._data_manager.get_next_for_processing()   # TODO: Ensure claim_work is synchronous

            if work is not None:
                processor = SimpleProcessor()
                self._busy_processors.append(processor)

                def on_complete(notifications: List[Notification]):
                    self._busy_processors.remove(processor)
                    self.on_processed(work, notifications)
                    self.on_information()

                processor.process(work, self._rules_manager.get_rules(), on_complete)

    def on_processed(self, file_update: FileUpdate, notifications: List[Notification]):
        if len(notifications) == 0:
            pass    # TODO
        else:
            for notification in notifications:
                self._notifier.add(notification)
            self._data_manager.mark_as_complete()    # TODO

    def _are_free_processors(self) -> bool:
        """
        Whether there are free data processors.
        :return: `True` if there are free processors, `False` otherwise
        """
        return len(self._idle_processors) - len(self._busy_processors)