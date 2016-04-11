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
from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster.common.models import Cookie
from cookiemonster.common.resource_accessor import ResourceAccessor
from cookiemonster.processor.models import Rule
from cookiemonster.tests.processor.example_enrichment_loader.hash_loader import SOURCE_NAME, KEY

MATCHES_ENIRCHED_COOKIE_WITH_IDENTIFIER = "/my/special/cookie"


def _matches(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    enrichment_from_source = cookie.get_most_recent_enrichment_from_source(SOURCE_NAME)
    if enrichment_from_source is None:
        return False
    return KEY in enrichment_from_source.metadata


def _action(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    return True


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _action, _priority)
register(_rule)
