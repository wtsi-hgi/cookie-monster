from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Notification, Rule, RuleAction
from cookiemonster.tests.processor.example_enrichment_loader.hash_loader import SOURCE_NAME, KEY

MATCHES_ENIRCHED_COOKIE_WITH_PATH = "/my/special/cookie"
NOTIFIES = "everyone"


def _matching_criteria(cookie: Cookie) -> bool:
    return cookie.get_metadata_by_source(SOURCE_NAME, KEY) is not None


def _action_generator(cookie: Cookie) -> RuleAction:
    return RuleAction([Notification(NOTIFIES, cookie.path)], True)


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matching_criteria, _action_generator, _priority)
register(_rule)