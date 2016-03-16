from cookiemonster.common.resource_accessor import ResourceAccessor
from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Notification, Rule, RuleAction

MATCHES_COOKIES_WITH_IDENTIFIER = "/my/special/cookie"
NOTIFIES = "everyone"


def _matches(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    return cookie.identifier == MATCHES_COOKIES_WITH_IDENTIFIER


def _generate_action(cookie: Cookie, resource_accessor: ResourceAccessor) -> RuleAction:
    return RuleAction([Notification(NOTIFIES, cookie.identifier)], True)


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
