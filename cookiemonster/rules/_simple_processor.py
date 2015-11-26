from typing import List, Callable

from cookiemonster.common.models import FileUpdate, Notification
from cookiemonster.rules._collections import RuleCollection
from cookiemonster.rules.processor import Processor


class SimpleProcessor(Processor):
    """
    Processor for a single file update.
    """
    def process(
            self, information: FileUpdate, rules: RuleCollection, on_complete: Callable[[List[Notification]], None]):
        """
        Processes the given file update.
        :param information: the file update to process
        :param rules: the rules to use when processing the file update
        :param on_complete: the on complete method that must be called when the processing has completed
        """
        pass


