from cookiemonster.common.resource_accessor import ResourceAccessor
from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Notification, Rule, RuleAction
from cookiemonster.tests.processor.example_enrichment_loader.hash_loader import SOURCE_NAME, KEY

MATCHES_ENIRCHED_COOKIE_WITH_IDENTIFIER = "/my/special/cookie"
NOTIFIES = "everyone"


def _matches(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    enrichment_from_source = cookie.get_most_recent_enrichment_from_source(SOURCE_NAME)
    if enrichment_from_source is None:
        return False
    return KEY in enrichment_from_source.metadata


def _generate_action(cookie: Cookie, resource_accessor: ResourceAccessor) -> RuleAction:
    return RuleAction([Notification(NOTIFIES, cookie.identifier)], True)


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
