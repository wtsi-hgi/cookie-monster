from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Rule, RuleAction


def _matching_criteria(cookie: Cookie) -> bool:
    return False


def _action_generator(cookie: Cookie) -> RuleAction:
    assert False


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matching_criteria, _action_generator, _priority)
register(_rule)