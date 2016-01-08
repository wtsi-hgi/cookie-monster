from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Notification, Rule, RuleAction

MATCHES_COOKIES_WITH_PATH = "/my/special/cookie"
NOTIFIES = "everyone"


def _matches(cookie: Cookie) -> bool:
    return cookie.path == MATCHES_COOKIES_WITH_PATH


def _generate_action(cookie: Cookie) -> RuleAction:
    return RuleAction([Notification(NOTIFIES, cookie.path)], True)


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
