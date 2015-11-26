from typing import Callable, List

from hgicommon.models import Model

from cookiemonster.common.models import FileUpdate, Notification
from cookiemonster.rules._collections import DataEnvironment


class RuleAction(Model):
    """
    A model of the action that has outcome from matching a rule.
    """
    def __init__(self, notifications: List[Notification], terminate_processing: bool):
        """
        Default constructor.
        :param notifications: list of notifications for external processes
        :param terminate_processing: whether the data processor should stop processing the update
        """
        self.notifications = notifications
        self.terminate_processing = terminate_processing


class Rule(Model):
    """
    Model of a rule that defines an action that should be executed if a criteria is matched.
    """
    def __init__(self,
                 matching_criteria: Callable[[DataEnvironment], bool],
                 action_generator: Callable[[DataEnvironment], RuleAction]):
        """
        Default constructor.
        :param matching_criteria: an arbitrary function that returns `True` if the rule is matched, given as input an
        immutable description of a file update and immutable, known data
        :param action_generator: an arbitrary function to execute if the matching criteria is met, which returns the
        rule's action, given as input an immutable description of a file update and immutable, known data
        """
        self.matching_criteria = matching_criteria
        self.action_generator = action_generator
