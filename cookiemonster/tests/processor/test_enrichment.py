import unittest
from datetime import datetime

from hgicommon.collections import Metadata
from hgicommon.mixable import Priority

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._models import EnrichmentLoader


class TestEnrichmentManager(unittest.TestCase):
    """
    Tests for `EnrichmentManager`.
    """
    def setUp(self):
        self.enrichment_loaders = [
            EnrichmentLoader(lambda *args: True, lambda *args: Enrichment("source_1", datetime.min, Metadata()), Priority.MIN_PRIORITY),
            EnrichmentLoader(lambda *args: False, lambda *args: Enrichment("source_2", datetime.min, Metadata()), Priority.get_lower_priority_value(Priority.MAX_PRIORITY)),
            EnrichmentLoader(lambda *args: False, lambda *args: Enrichment("source_3", datetime.min, Metadata()), Priority.MAX_PRIORITY)
        ]
        self.cookie = Cookie("path")

    def test_next_enrichment(self):
        enrichment_manager = EnrichmentManager(self.enrichment_loaders)
        enrichment = enrichment_manager.next_enrichment(self.cookie)
        self.assertEquals(enrichment, self.enrichment_loaders[2].load_enrichment(self.cookie))


if __name__ == "__main__":
    unittest.main()
