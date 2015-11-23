import unittest
from datetime import datetime

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import Metadata, FileUpdate


class TestFileUpdateCollection(unittest.TestCase):
    """
    Tests for `FileUpdateCollection`.
    """
    def test_empty_as_default(self):
        self.assertEquals(len(FileUpdateCollection()), 0)

    def test_can_instantiate_with_list(self):
        file_updates_list = [FileUpdate("", "", datetime.min, Metadata()), FileUpdate("", "", datetime.min, Metadata())]
        file_updates = FileUpdateCollection(file_updates_list)
        self.assertCountEqual(file_updates, file_updates_list)

    def test_can_instantiate_with_file_update_collection(self):
        file_updates1= FileUpdateCollection([FileUpdate("", "", datetime.min, Metadata()), FileUpdate("", "", datetime.min, Metadata())])
        file_updates2 = FileUpdateCollection(file_updates1)
        self.assertCountEqual(file_updates2, file_updates1)

    def test_get_most_recent_when_empty(self):
        self.assertRaises(ValueError, FileUpdateCollection().get_most_recent)

    def test_get_most_recent(self):
        file_updates = FileUpdateCollection([
            FileUpdate("", "", datetime.max, Metadata()),
            FileUpdate("", "", datetime.min, Metadata())
        ])
        self.assertCountEqual(file_updates.get_most_recent(), [file_updates[0]])

    def test_get_most_recent_when_many_have_same_latest(self):
        file_updates = FileUpdateCollection([
            FileUpdate("", "", datetime.max, Metadata()),
            FileUpdate("", "", datetime.min, Metadata()),
            FileUpdate("", "", datetime.max, Metadata())
        ])
        self.assertCountEqual(file_updates.get_most_recent(), [file_updates[0], file_updates[2]])


if __name__ == '__main__':
    unittest.main()
