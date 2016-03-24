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

from hgicommon.collections import Metadata

from cookiemonster.common.models import Update
from cookiemonster.common.collections import UpdateCollection


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


if __name__ == "__main__":
    unittest.main()
