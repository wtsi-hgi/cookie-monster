import unittest
from datetime import datetime

from hgicommon.collections import Metadata
from hgicommon.data_source import ListDataSource
from hgicommon.mixable import Priority

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.processor._enrichment import EnrichmentManager, EnrichmentLoaderSource
from cookiemonster.processor.models import EnrichmentLoader


class TestEnrichmentManager(unittest.TestCase):
    """
    Tests for `EnrichmentManager`.
    """
    def setUp(self):
        self.enrichment_loaders = [
            EnrichmentLoader(lambda *args: False, lambda *args: Enrichment("source_1", datetime.min, Metadata()),
                             Priority.MIN_PRIORITY),
            EnrichmentLoader(lambda *args: True, lambda *args: Enrichment("source_2", datetime.min, Metadata()),
                             Priority.get_lower_priority_value(Priority.MAX_PRIORITY)),
            EnrichmentLoader(lambda *args: True, lambda *args: Enrichment("source_3", datetime.min, Metadata()),
                             Priority.MAX_PRIORITY)
        ]
        self.cookie = Cookie("path")

    def test_next_enrichment(self):
        enrichment_manager = EnrichmentManager(ListDataSource(self.enrichment_loaders))
        enrichment = enrichment_manager.next_enrichment(self.cookie)
        self.assertEquals(enrichment, self.enrichment_loaders[2].load_enrichment(self.cookie))


class TestEnrichmentLoaderSource(unittest.TestCase):
    """
    Tests for `EnrichmentLoaderSource`.
    """
    def setUp(self):
        self.source = EnrichmentLoaderSource("/")

    def test_is_data_file_when_is(self):
        self.assertTrue(self.source.is_data_file("/my/file.loader.py"))

    def test_is_data_file_when_is_not(self):
        self.assertFalse(self.source.is_data_file("/my/file.py"))


if __name__ == "__main__":
    unittest.main()
