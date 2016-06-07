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
import logging
import unittest
from datetime import datetime
from queue import PriorityQueue
from typing import Iterable
from unittest.mock import MagicMock

from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.processor._enrichment import EnrichmentManager, EnrichmentLoaderSource
from cookiemonster.processor.models import EnrichmentLoader


_ENRICHMENT_IDENTIFIER = "my_enrichment"


class TestEnrichmentManager(unittest.TestCase):
    """
    Tests for `EnrichmentManager`.
    """
    def setUp(self):
        self.enrichment_loaders = [
            EnrichmentLoader(lambda *args: False, lambda *args: Enrichment(
                "source_1", datetime.min, Metadata()), _ENRICHMENT_IDENTIFIER, 10),
            EnrichmentLoader(lambda *args: True, lambda *args: Enrichment(
                "source_2", datetime.min, Metadata()), _ENRICHMENT_IDENTIFIER, 5),
            EnrichmentLoader(lambda *args: True, lambda *args: Enrichment(
                "source_3", datetime.min, Metadata()), _ENRICHMENT_IDENTIFIER, 2),
            EnrichmentLoader(lambda *args: False, lambda *args: Enrichment(
                "source_4", datetime.min, Metadata()), _ENRICHMENT_IDENTIFIER, 1)
        ]

    def test_next_enrichment(self):
        enrichment_manager = EnrichmentManager(self.enrichment_loaders)
        self._test_loaded_in_correct_order(enrichment_manager, self.enrichment_loaders)

    def test_resilience_to_broken_enrichment_loaders(self):
        def faulty_load_enrichment(i: int) -> Enrichment:
            raise RuntimeError()

        def faulty_can_enrich(i: int) -> bool:
            raise RuntimeError()

        additional_enrichment_loaders = [
            EnrichmentLoader(lambda *args: True, lambda *args: faulty_load_enrichment(1), 0),
            EnrichmentLoader(lambda *args: faulty_can_enrich(1), lambda *args: Enrichment(
                "", datetime.min, Metadata()), _ENRICHMENT_IDENTIFIER, 5),
            EnrichmentLoader(lambda *args: True, lambda *args: faulty_load_enrichment(2), _ENRICHMENT_IDENTIFIER, 15)
        ]
        enrichment_manager = EnrichmentManager(additional_enrichment_loaders + self.enrichment_loaders)

        self._test_loaded_in_correct_order(enrichment_manager, self.enrichment_loaders)

    def _test_loaded_in_correct_order(
            self, enrichment_manager: EnrichmentManager, enrichment_loaders: Iterable[EnrichmentLoader]):
        """
        Tests that the given enrichment manager applies enrichments defined be the given loaders in the correct order.
        :param enrichment_manager: enrichment manager
        :param enrichment_loaders: enrichment loaders
        """
        logging.root.setLevel(logging.CRITICAL)
        cookie = Cookie("the_identifier")

        enrichment_loaders_priority_queue = PriorityQueue()
        for enrichment_loader in enrichment_loaders:
            if enrichment_loader.can_enrich(cookie):
                enrichment_loaders_priority_queue.put(enrichment_loader)

        enrichment = enrichment_manager.next_enrichment(cookie)
        while enrichment is not None:
            expected_enrichment_loader = enrichment_loaders_priority_queue.get()    # type: EnrichmentLoader
            expected_enrichment = expected_enrichment_loader.load_enrichment(cookie)
            self.assertEqual(enrichment, expected_enrichment)
            cookie.enrich(enrichment)
            expected_enrichment_loader.can_enrich = MagicMock(return_value=False)
            enrichment = enrichment_manager.next_enrichment(cookie)
        self.assertTrue(enrichment_loaders_priority_queue.empty())


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
