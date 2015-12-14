from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Notification, Rule, RuleAction

MATCHES_COOKIES_WITH_PATH = "/my/special/cookie"
NOTIFIES = "everyone"


def _matching_criteria(cookie: Cookie) -> bool:
    return cookie.path == MATCHES_COOKIES_WITH_PATH


def _action_generator(cookie: Cookie) -> RuleAction:
    return RuleAction([Notification(NOTIFIES, cookie.path)], True)


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matching_criteria, _action_generator, _priority)
register(_rule)