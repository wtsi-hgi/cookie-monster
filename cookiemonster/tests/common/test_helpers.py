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
import unittest
from datetime import datetime

from cookiemonster.common.collections import UpdateCollection, EnrichmentCollection
from cookiemonster.common.helpers import get_enrichment_changes_from_source
from cookiemonster.common.models import Update, Enrichment, EnrichmentDiff
from hgicommon.collections import Metadata


class TestEnrichmentCollection(unittest.TestCase):
    """
    Tests for `EnrichmentCollection`.
    """
    def setUp(self):
        self.enrichments = EnrichmentCollection()

    def test_enrichment_no_diff(self):
        self.enrichments.add(Enrichment('source', datetime(1, 1, 1), Metadata()))
        self.enrichments.add(Enrichment('source', datetime(2, 2, 2), Metadata()))

        diffs = get_enrichment_changes_from_source(self.enrichments, 'source')
        self.assertEqual(len(diffs), 0)

    def test_enrichment_diff(self):
        self.enrichments.add(Enrichment('source', datetime(1, 1, 1), Metadata({'foo': 123, 'bar': 456, 'quux': 789})))
        self.enrichments.add(Enrichment('source', datetime(2, 2, 2), Metadata({'xyz': 123, 'bar': 456, 'quux': 999})))

        diffs = get_enrichment_changes_from_source(self.enrichments, 'source')
        self.assertEqual(len(diffs), 1)

        diff = diffs[0]
        self.assertIsInstance(diff, EnrichmentDiff)
        self.assertEqual(diff.source, 'source')
        self.assertEqual(diff.timestamp, datetime(2, 2, 2))
        self.assertTrue(diff.is_different())
        self.assertEqual(diff.additions, Metadata({'xyz': 123, 'quux': 999}))
        self.assertEqual(diff.deletions, Metadata({'foo': 123, 'quux': 789}))

    def test_enrichment_diff_by_key(self):
        self.enrichments.add(Enrichment('source', datetime(1, 1, 1), Metadata({'foo': 123, 'bar': 456, 'quux': 789})))
        self.enrichments.add(Enrichment('source', datetime(2, 2, 2), Metadata({'xyz': 123, 'bar': 456, 'quux': 999})))

        diffs = get_enrichment_changes_from_source(self.enrichments, 'source', 'foo')
        self.assertEqual(len(diffs), 1)

        diff = diffs[0]
        self.assertEqual(diff.additions, Metadata())
        self.assertEqual(diff.deletions, Metadata({'foo': 123}))

    def test_enrichment_diff_by_keys(self):
        self.enrichments.add(Enrichment('source', datetime(1, 1, 1), Metadata({'foo': 123, 'bar': 456, 'quux': 789})))
        self.enrichments.add(Enrichment('source', datetime(2, 2, 2), Metadata({'xyz': 123, 'bar': 456, 'quux': 999})))

        diffs = get_enrichment_changes_from_source(self.enrichments, 'source', ['bar', 'quux'])
        self.assertEqual(len(diffs), 1)

        diff = diffs[0]
        self.assertEqual(diff.additions, Metadata({'quux': 999}))
        self.assertEqual(diff.deletions, Metadata({'quux': 789}))

    def test_enrichment_diff_from_timestamp(self):
        self.enrichments.add(Enrichment('source', datetime(1, 1, 1), Metadata()))
        self.enrichments.add(Enrichment('source', datetime(2, 2, 2), Metadata({'foo': 123})))
        self.enrichments.add(Enrichment('source', datetime(3, 3, 3), Metadata({'bar': 123})))

        all_diffs = get_enrichment_changes_from_source(self.enrichments, 'source')
        self.assertEqual(len(all_diffs), 2)

        since_diffs = get_enrichment_changes_from_source(self.enrichments, 'source', since=datetime(3, 3, 3))
        self.assertEqual(len(since_diffs), 1)

        diff = since_diffs[0]
        self.assertEqual(diff.timestamp, datetime(3, 3, 3))
        self.assertEqual(diff.additions, Metadata({'bar': 123}))
        self.assertEqual(diff.deletions, Metadata({'foo': 123}))


if __name__ == "__main__":
    unittest.main()
