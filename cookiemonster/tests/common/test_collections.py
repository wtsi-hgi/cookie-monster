import unittest
from datetime import datetime

from hgicommon.collections import Metadata

from cookiemonster.common.models import Update
from cookiemonster.common.collections import UpdateCollection


class TestFileUpdateCollection(unittest.TestCase):
    """
    Tests for `UpdateCollection`.
    """
    def setUp(self):
        self.metadata = Metadata()

    def test_empty_as_default(self):
        self.assertEquals(len(UpdateCollection()), 0)

    def test_can_instantiate_with_list(self):
        file_updates_list = [
            Update("", "", datetime.min, self.metadata),
            Update("", "", datetime.min, self.metadata)
        ]
        file_updates = UpdateCollection(file_updates_list)
        self.assertCountEqual(file_updates, file_updates_list)

    def test_can_instantiate_with_file_update_collection(self):
        file_updates1= UpdateCollection([
            Update("", "", datetime.min, self.metadata),
            Update("", "", datetime.min, self.metadata)
        ])
        file_updates2 = UpdateCollection(file_updates1)
        self.assertCountEqual(file_updates2, file_updates1)

    def test_get_most_recent_when_empty(self):
        self.assertRaises(ValueError, UpdateCollection().get_most_recent)

    def test_get_most_recent(self):
        file_updates = UpdateCollection([
            Update("", "", datetime.max, self.metadata),
            Update("", "", datetime.min, self.metadata)
        ])
        self.assertCountEqual(file_updates.get_most_recent(), [file_updates[0]])

    def test_get_most_recent_when_many_have_same_latest(self):
        file_updates = UpdateCollection([
            Update("", "", datetime.max, self.metadata),
            Update("", "", datetime.min, self.metadata),
            Update("", "", datetime.max, self.metadata)
        ])
        self.assertCountEqual(file_updates.get_most_recent(), [file_updates[0], file_updates[2]])


if __name__ == "__main__":
    unittest.main()
