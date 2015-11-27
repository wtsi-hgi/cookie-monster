from typing import Callable, Set

from hgicommon.models import Model

from cookiemonster.common.models import Notification, CookieProcessState, Cookie, CookieCrumbs


class RuleAction(Model):
    """
    A model of the action that has outcome from matching a rule.
    """
    def __init__(self, notifications: Set[Notification], terminate_processing: bool):
        """
        Default constructor.
        :param notifications: set of notifications for external processes
        :param terminate_processing: whether the data processor should stop processing the update
        """
        self.notifications = notifications
        self.terminate_processing = terminate_processing


class Rule(Model):
    """
    A model of a rule that defines an action that should be executed if a criteria is matched.
    """
    def __init__(self, matching_criteria: Callable[[CookieProcessState], bool],
                 action_generator: Callable[[CookieProcessState], RuleAction]):
        """
        Default constructor.
        :param matching_criteria: see `Rule.matching_criteria`
        :param action_generator: see `Rule.action_generator`
        """
        self._matching_criteria = matching_criteria
        self._action_generator = action_generator

    def matching_criteria(self, job: CookieProcessState) -> bool:
        """
        Returns whether this rule applies to the given job that is being processed
        :param job: the job to check if the rule applies to
        :return: whether the rule applies
        """
        return self._matching_criteria(job)

    def action_generator(self, job) -> RuleAction:
        """
        Returns the action that should be taken in response to the given job.

        Will raise a `ValueError` if the rule does not match the given job
        :param job: the job to generate an action for
        :return: the generated action
        """
        if not self.matching_criteria(job):
            return ValueError("Rules does not match job: %s" % job)
        return self._action_generator(job)


class DataLoader(Model):
    """
    TODO
    """
    def __init__(self, is_already_known: Callable[[Cookie], bool], load: Callable[[Cookie], CookieCrumbs]):
        """
        Default constructor.
        :param is_already_known: see `DataLoader.is_already_known`
        :param load: see `DataLoader.load`
        """
        self._is_already_known = is_already_known
        self._load = load

    def is_already_known(self, known_data: Cookie) -> bool:
        """
        Returns whether or not the data that this loader can load is already in the set of given, known data.
        :param known_data: the data already known
        :return: whether the data is already in the set of known data
        """
        return self._is_already_known(known_data)

    def load(self, known_data: Cookie) -> CookieCrumbs:
        """
        Load data that can be added to a set of known data.
        :param known_data: the pre-existing set of known data
        :return: the loaded data
        """
        return self._load(known_data)
