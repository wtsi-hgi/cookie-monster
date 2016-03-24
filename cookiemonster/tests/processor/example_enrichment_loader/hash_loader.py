"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
from datetime import datetime

from cookiemonster.common.resource_accessor import ResourceAccessor
from hgicommon.collections import Metadata

from cookiemonster import EnrichmentLoader, Cookie, Enrichment
from hgicommon.mixable import Priority
from hgicommon.data_source import register


SOURCE_NAME = "hash_loader"
KEY = "hash"


def _can_enrich(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    return SOURCE_NAME not in [enrichment.source for enrichment in cookie.enrichments]


def _load_enrichment(cookie: Cookie, resource_accessor: ResourceAccessor) -> Enrichment:
    return Enrichment(SOURCE_NAME, datetime.min, Metadata({KEY: hash(cookie.identifier)}))


_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, _priority)
register(_enrichment_loader)
