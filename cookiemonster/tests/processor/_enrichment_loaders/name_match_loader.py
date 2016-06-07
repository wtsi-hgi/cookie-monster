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
from datetime import datetime
from unittest.mock import MagicMock

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.common.context import Context
from hgicommon.collections import Metadata

from hgicommon.mixable import Priority
from hgicommon.data_source import register

from cookiemonster.processor.models import EnrichmentLoader

NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID = "name_match_loader"
NAME_ENRICHMENT_LOADER_MATCH_COOKIE = "/cookie/matches/name/for/enrichment"


def _can_enrich(cookie: Cookie, context: Context) -> bool:
    return cookie.identifier == NAME_ENRICHMENT_LOADER_MATCH_COOKIE \
           and NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID not in [enrichment.source for enrichment in cookie.enrichments]


def _load_enrichment(cookie: Cookie, context: Context) -> Enrichment:
    return Enrichment(NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID, datetime.min, Metadata({"matches": True}))


_priority = Priority.get_lower_priority_value(Priority.MAX_PRIORITY)

_enrichment_loader = EnrichmentLoader(MagicMock(side_effect=_can_enrich), MagicMock(side_effect=_load_enrichment),
                                      NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID, _priority)
register(_enrichment_loader)
