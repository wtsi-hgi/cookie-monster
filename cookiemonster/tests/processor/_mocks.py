from unittest.mock import MagicMock

from hgicommon.mixable import Priority

from cookiemonster.common.models import Notification
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
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


def create_magic_mock_cookie_jar() -> CookieJar:
    """
    Creates a magic mock CookieJar - has the implementation of a CookieJar all methods are implemented using magic mocks
    and therefore their usage is recorded.
    :return: the created magic mock
    """
    cookie_jar = InMemoryCookieJar()
    original_get_next_for_processing = cookie_jar.get_next_for_processing
    original_enrich_cookie = cookie_jar.enrich_cookie
    original_mark_as_failed = cookie_jar.mark_as_complete
    original_mark_as_completed = cookie_jar.mark_as_complete
    original_mark_as_reprocess = cookie_jar.mark_as_reprocess
    cookie_jar.get_next_for_processing = MagicMock(side_effect=original_get_next_for_processing)
    cookie_jar.enrich_cookie = MagicMock(side_effect=original_enrich_cookie)
    cookie_jar.mark_as_failed = MagicMock(side_effect=original_mark_as_failed)
    cookie_jar.mark_as_complete = MagicMock(side_effect=original_mark_as_completed)
    cookie_jar.mark_as_reprocess = MagicMock(side_effect=original_mark_as_reprocess)
    return cookie_jar