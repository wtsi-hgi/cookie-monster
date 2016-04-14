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
from cookiemonster.common.resource_accessor import ResourceAccessor
from hgicommon.collections import Metadata

from hgicommon.mixable import Priority
from hgicommon.data_source import register

from cookiemonster.processor.models import EnrichmentLoader

HASH_ENRICHMENT_LOADER_ID = "hash_loader"
KEY = "hash"


def _can_enrich(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    return HASH_ENRICHMENT_LOADER_ID not in [enrichment.source for enrichment in cookie.enrichments]


def _load_enrichment(cookie: Cookie, resource_accessor: ResourceAccessor) -> Enrichment:
    return Enrichment(HASH_ENRICHMENT_LOADER_ID, datetime.min, Metadata({KEY: hash(cookie.identifier)}))


_priority = Priority.MIN_PRIORITY

_enrichment_loader = EnrichmentLoader(MagicMock(side_effect=_can_enrich), MagicMock(side_effect=_load_enrichment),
                                      _priority, HASH_ENRICHMENT_LOADER_ID)
register(_enrichment_loader)
