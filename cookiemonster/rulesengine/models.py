from typing import Callable, List, Any
from hgicommon.models import Model

from cookiemonster.common.models import FileUpdate
from cookiemonster.rulesengine.collections import DataEnvironment


class Rule(Model):
    """
    Model of a rule that defines an action that should be executed if a criteria is matched.
    """
    def __init__(self,
                 matching_criteria: Callable[[FileUpdate, DataEnvironment], bool],
                 action: Callable[[FileUpdate, DataEnvironment], Decision]):
        """
        Default constructor.
        :param matching_criteria: an arbitrary function that returns `True` if the rule is matched, given as input an
        immutable description of a file update and immutable, known data
        :param action: an arbitrary function to execute if the matching criteria is met, which returns a `Decision`,
        given as input an immutable description of a file update and immutable, known data
        """
        raise NotImplementedError()


class Notification(Model):
    """
    A model of a notification that should be sent to an external process.
    """
    def __init__(self, external_process_name: str, data: Any=None):
        """
        Default constructor.
        :param external_process_name: the name of the external process that should be informed
        :param data: the data (if any) that should be given to the external process
        """
        raise NotImplementedError()


class Decision(Model):
    """
    A model of the decision that has outcome from matching a rule.
    """
    def __init__(self, notifications: List[Notification], terminate_processing: bool):
        """
        Default constructor.
        :param notifications: list of notifications for external processes
        :param terminate_processing: whether the data processor should stop processing the update
        """
        raise NotImplementedError()

