import unittest
from datetime import datetime

from hgicommon.collections import Metadata

from cookiemonster import Cookie, Enrichment


class TestCookie(unittest.TestCase):
    """
    Tests for `Cookie`.
    """
    _IDENTIFIER = "id"

    def setUp(self):
        self._cookie = Cookie(TestCookie._IDENTIFIER)

    def test_enrich_with_no_previous(self):
        enrichment = Enrichment("source", datetime(1, 2, 3), Metadata())
        self._cookie.enrich(enrichment)
        self.assertEqual(self._cookie.enrichments, [enrichment])

    def test_enrich_with_older_previous(self):
        older_enrichment = Enrichment("source", datetime(1, 1, 1), Metadata())
        self._cookie.enrich(older_enrichment)
        enrichment = Enrichment("source", datetime(2, 2, 2), Metadata())
        self._cookie.enrich(enrichment)
        self.assertEqual(self._cookie.enrichments, [older_enrichment, enrichment])

    def test_enrich_with_newer_previous(self):
        newer_enrichment = Enrichment("source", datetime(3, 3, 3), Metadata())
        self._cookie.enrich(newer_enrichment)
        enrichment = Enrichment("source", datetime(2, 2, 2), Metadata())
        self._cookie.enrich(enrichment)
        self.assertEqual(self._cookie.enrichments, [enrichment, newer_enrichment])

    def test_enrich_with_older_and_newer_previous(self):
        older_enrichment = Enrichment("source", datetime(1, 1, 1), Metadata())
        self._cookie.enrich(older_enrichment)
        newer_enrichment = Enrichment("source", datetime(3, 3, 3), Metadata())
        self._cookie.enrich(newer_enrichment)
        enrichment = Enrichment("source", datetime(2, 2, 2), Metadata())
        self._cookie.enrich(enrichment)
        self.assertEqual(self._cookie.enrichments, [older_enrichment, enrichment, newer_enrichment])

    def test_get_most_recent_enrichment_from_source_when_none_from_source(self):
        self.assertIsNone(self._cookie.get_most_recent_enrichment_from_source("source"))

    def test_get_most_recent_enrichment_from_source_when_multiple_from_source(self):
        older_enrichment = Enrichment("source", datetime(1, 1, 1), Metadata())
        self._cookie.enrich(older_enrichment)
        newer_enrichment = Enrichment("source", datetime(2, 2, 2), Metadata())
        self._cookie.enrich(newer_enrichment)
        other_enrichment = Enrichment("other", datetime(3, 3, 3), Metadata())
        self._cookie.enrich(other_enrichment)
        self.assertEqual(self._cookie.get_most_recent_enrichment_from_source("source"), newer_enrichment)

    def test_get_enrichment_sources_when_no_enrichments(self):
        self.assertEqual(len(self._cookie.get_enrichment_sources()), 0)

    def test_get_enrichment_sources_when_enrichments(self):
        self._cookie.enrich(Enrichment("source_1", datetime(1, 1, 1), Metadata()))
        self._cookie.enrich(Enrichment("source_1", datetime(2, 1, 1), Metadata()))
        self._cookie.enrich(Enrichment("source_2", datetime(1, 1, 1), Metadata()))
        self.assertCountEqual(self._cookie.get_enrichment_sources(), ["source_1", "source_2"])


if __name__ == "__main__":
    unittest.main()
