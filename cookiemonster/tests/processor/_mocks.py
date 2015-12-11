from hgicommon.mixable import Priority

from cookiemonster.common.models import Notification
from cookiemonster.processor.models import Rule
from cookiemonster.processor.models import RuleAction


def create_mock_rule(priority: int=Priority.MIN_PRIORITY) -> Rule:
    """
    Creates a mock `Rule` object.
    :param priority: (optional) the priority of the rule
    :return: the created rule
    """
    return Rule(
        lambda file_update, data_environment: True,
        lambda file_update, data_environment: RuleAction([Notification("")], True),
        priority
    )
