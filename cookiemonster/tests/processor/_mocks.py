"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from unittest.mock import MagicMock

from hgicommon.mixable import Priority

from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.processor.models import Rule


def create_mock_rule(priority: int=Priority.MIN_PRIORITY) -> Rule:
    """
    Creates a mock `Rule` object.
    :param priority: (optional) the priority of the rule
    :return: the created rule
    """
    return Rule(
        lambda file_update, data_environment: True,
        lambda file_update, data_environment: True,
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
    original_mark_as_reprocess = cookie_jar.mark_for_processing
    cookie_jar.get_next_for_processing = MagicMock(side_effect=original_get_next_for_processing)
    cookie_jar.enrich_cookie = MagicMock(side_effect=original_enrich_cookie)
    cookie_jar.mark_as_failed = MagicMock(side_effect=original_mark_as_failed)
    cookie_jar.mark_as_complete = MagicMock(side_effect=original_mark_as_completed)
    cookie_jar.mark_for_processing = MagicMock(side_effect=original_mark_as_reprocess)
    return cookie_jar
