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

from cookiemonster.common.models import Cookie
from cookiemonster.common.context import Context
from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster.processor.models import Rule

NO_MATCH_RULE_ID = "no_match_rule"

def _matches(cookie: Cookie, context: Context) -> bool:
    return False


def _action(cookie: Cookie, context: Context) -> bool:
    assert False


_priority = Priority.MAX_PRIORITY

_rule = Rule(MagicMock(side_effect=_matches), MagicMock(side_effect=_action), NO_MATCH_RULE_ID, _priority)
register(_rule)
