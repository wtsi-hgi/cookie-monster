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
from datetime import datetime, timedelta

from cookiemonster.common.collections import UpdateCollection, EnrichmentCollection
from cookiemonster.common.models import Update, Enrichment
from hgicommon.collections import Metadata

_ENRICHMENT_1 = Enrichment("source", datetime(1, 1, 1), Metadata())
_ENRICHMENT_2 = Enrichment("source", datetime(2, 2, 2), Metadata())
_ENRICHMENT_3 = Enrichment("source", datetime(3, 3, 3), Metadata())


class TestUpdateCollection(unittest.TestCase):
    """
    Tests for `UpdateCollection`.
    """
    _TARGET = "/my/target"

    def setUp(self):
        self.metadata = Metadata()

    def test_empty_as_default(self):
        self.assertEqual(len(UpdateCollection()), 0)

    def test_can_instantiate_with_list(self):
        updates_list = [
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata)
        ]
        updates = UpdateCollection(updates_list)
        self.assertCountEqual(updates, updates_list)

    def test_can_instantiate_with_update_collection(self):
        updates_1= UpdateCollection([
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata)
        ])
        updates_2 = UpdateCollection(updates_1)
        self.assertCountEqual(updates_2, updates_1)

    def test_get_most_recent_when_empty(self):
        self.assertRaises(ValueError, UpdateCollection().get_most_recent)

    def test_get_most_recent(self):
        updates = UpdateCollection([
            Update(TestUpdateCollection._TARGET, datetime.max, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata)
        ])
        self.assertCountEqual(updates.get_most_recent(), [updates[0]])

    def test_get_most_recent_when_many_have_same_latest(self):
        updates = UpdateCollection([
            Update(TestUpdateCollection._TARGET, datetime.max, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.max, self.metadata)
        ])
        self.assertCountEqual(updates.get_most_recent(), [updates[0], updates[2]])

    def test_get_entity_updates(self):
        updates = UpdateCollection([
            Update(TestUpdateCollection._TARGET, datetime.max, self.metadata),
            Update(TestUpdateCollection._TARGET + "/other", datetime.max, self.metadata),
            Update(TestUpdateCollection._TARGET, datetime.min, self.metadata)
        ])
        self.assertCountEqual(updates.get_entity_updates(TestUpdateCollection._TARGET), [updates[0], updates[2]])


class TestEnrichmentCollection(unittest.TestCase):
    """
    Tests for `EnrichmentCollection`.
    """
    assert _ENRICHMENT_1.timestamp < _ENRICHMENT_2.timestamp < _ENRICHMENT_3.timestamp

    def setUp(self):
        self.enrichments = EnrichmentCollection()

    def test_init_with_enrichments(self):
        enrichments = EnrichmentCollection([_ENRICHMENT_2, _ENRICHMENT_3, _ENRICHMENT_1])
        self.assertSequenceEqual(enrichments, [_ENRICHMENT_1, _ENRICHMENT_2, _ENRICHMENT_3])

    def test_add_with_no_previous(self):
        self.enrichments.add(_ENRICHMENT_1)
        self.assertSequenceEqual(self.enrichments, [_ENRICHMENT_1])

    def test_add_with_older_previous(self):
        self.enrichments.add(_ENRICHMENT_1)
        self.enrichments.add(_ENRICHMENT_2)
        self.assertSequenceEqual(self.enrichments, [_ENRICHMENT_1, _ENRICHMENT_2])

    def test_add_with_newer_previous(self):
        self.enrichments.add(_ENRICHMENT_2)
        self.enrichments.add(_ENRICHMENT_1)
        self.assertSequenceEqual(self.enrichments, [_ENRICHMENT_1, _ENRICHMENT_2])

    def test_add_with_older_and_newer_previous(self):
        self.enrichments.add(_ENRICHMENT_1)
        self.enrichments.add(_ENRICHMENT_3)
        self.enrichments.add(_ENRICHMENT_2)
        self.assertSequenceEqual(self.enrichments, [_ENRICHMENT_1, _ENRICHMENT_2, _ENRICHMENT_3])

    def test_add_multiple(self):
        self.enrichments.add([_ENRICHMENT_3, _ENRICHMENT_1, _ENRICHMENT_2])
        self.assertSequenceEqual(self.enrichments, [_ENRICHMENT_1, _ENRICHMENT_2, _ENRICHMENT_3])

    def test_get_most_recent_from_source_when_none_from_source(self):
        self.assertIsNone(self.enrichments.get_most_recent_from_source("other_source"))

    def test_get_most_recent_from_source_when_multiple_from_source(self):
        assert _ENRICHMENT_1.source == _ENRICHMENT_2.source == _ENRICHMENT_3.source
        self.enrichments.add([_ENRICHMENT_1, _ENRICHMENT_2, _ENRICHMENT_3])
        self.assertEqual(self.enrichments.get_most_recent_from_source(_ENRICHMENT_1.source), _ENRICHMENT_3)

    def test_get_all_since_enrichment_from_source_when_no_enrichments(self):
        enrichments = self.enrichments.get_all_since_enrichment_from_source(_ENRICHMENT_1.source)
        self.assertEqual(len(enrichments), 0)

    def test_get_all_since_enrichment_from_source_when_no_enrichments_from_source(self):
        self.enrichments.add(_ENRICHMENT_1)
        enrichments = self.enrichments.get_all_since_enrichment_from_source("other_source")
        self.assertCountEqual(enrichments, [_ENRICHMENT_1])

    def test_get_all_since_enrichment_from_source_when_only_enrichment_from_source(self):
        assert _ENRICHMENT_1.source == _ENRICHMENT_2.source
        self.enrichments.add([_ENRICHMENT_1, _ENRICHMENT_2])
        enrichments = self.enrichments.get_all_since_enrichment_from_source(_ENRICHMENT_1.source)
        self.assertEqual(len(enrichments), 0)

    def test_get_all_since_enrichment_from_source_when_no_after_enrichment_from_source(self):
        assert _ENRICHMENT_1.source == _ENRICHMENT_2.source
        self.enrichments.add([_ENRICHMENT_1, Enrichment("other_source", _ENRICHMENT_2.timestamp, Metadata())])
        enrichments = self.enrichments.get_all_since_enrichment_from_source("other_source")
        self.assertEqual(len(enrichments), 0)

    def test_get_all_since_enrichment_from_source_when_one_enrichment_from_source_and_enrichments_afterwards(self):
        delta = timedelta(seconds=1)
        assert _ENRICHMENT_2.timestamp - _ENRICHMENT_1.timestamp > delta
        self.enrichments.add([
            _ENRICHMENT_1,
            Enrichment("other_source", _ENRICHMENT_1.timestamp + delta, Metadata()),
            _ENRICHMENT_2,
            _ENRICHMENT_3
        ])
        enrichments = self.enrichments.get_all_since_enrichment_from_source("other_source")
        self.assertCountEqual(enrichments, [_ENRICHMENT_2, _ENRICHMENT_3])

    def test_get_all_since_enrichment_from_source_when_multiple_enrichment_from_source_and_enrichments_afterwards(self):
        delta = timedelta(seconds=1)
        assert _ENRICHMENT_2.timestamp - _ENRICHMENT_1.timestamp > delta
        assert _ENRICHMENT_3.timestamp - _ENRICHMENT_2.timestamp > delta
        source = "other_source"
        self.enrichments.add([
            _ENRICHMENT_1,
            Enrichment(source, _ENRICHMENT_1.timestamp + delta, Metadata()),
            _ENRICHMENT_2,
            Enrichment(source, _ENRICHMENT_2.timestamp + delta, Metadata()),
            _ENRICHMENT_3
        ])
        enrichments = self.enrichments.get_all_since_enrichment_from_source(source)
        self.assertCountEqual(enrichments, [_ENRICHMENT_3])


if __name__ == "__main__":
    unittest.main()
