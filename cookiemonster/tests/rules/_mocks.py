from cookiemonster.common.models import Notification
from cookiemonster.rules._models import Rule
from cookiemonster.rules._models import RuleAction


def create_mock_rule(distinguisher: str="") -> Rule:
    """
    Creates a mock `Rule` object.
    :param distinguisher: a value that can be used to distinguiush this rule from another
    :return: the created rule
    """
    return Rule(
        lambda file_update, data_environment: True,
        lambda file_update, data_environment: RuleAction([Notification(distinguisher)], True)
    )