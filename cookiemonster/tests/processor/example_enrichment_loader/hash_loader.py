from datetime import datetime

from hgicommon.collections import Metadata

from cookiemonster import EnrichmentLoader, Cookie, Enrichment
from hgicommon.mixable import Priority
from hgicommon.data_source import register


SOURCE_NAME = "hash_loader"
KEY = "hash"


def _can_enrich(cookie: Cookie) -> bool:
    return SOURCE_NAME in [enrichment.source for enrichment in cookie.enrichments]


def _load_enrichment(cookie: Cookie) -> Enrichment:
    return Enrichment(SOURCE_NAME, datetime.min, Metadata({KEY: hash(cookie.path)}))


_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, _priority)
register(_enrichment_loader)