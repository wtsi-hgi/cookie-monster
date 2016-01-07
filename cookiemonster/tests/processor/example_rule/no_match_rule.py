from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Rule, RuleAction


def _matches(cookie: Cookie) -> bool:
    return False


def _generate_action(cookie: Cookie) -> RuleAction:
    assert False


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)