from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import EnrichmentLoader, Cookie, Enrichment


def _can_enrich(cookie: Cookie) -> bool:
    return False


def _load_enrichment(cookie: Cookie) -> Enrichment:
    assert False


_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, _priority)
register(_enrichment_loader)
