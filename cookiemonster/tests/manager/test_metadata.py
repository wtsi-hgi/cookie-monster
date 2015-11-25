import unittest
from unittest import mock

from cookiemonster.manager._metadata import MetadataDB


class TestMetadataDB(unittest.TestCase):
    _DB   = 'foo'
    _HOST = 'bar'

    @mock.patch('cookiemonster.manager._metadata.couchdb')
    def test_metadata_init(self, mock_couch):
        # Connect and create
        mock_couch.Server().__contains__.return_value = False
        _ = MetadataDB(TestMetadataDB._DB, TestMetadataDB._HOST)

        mock_couch.Server.assert_called_with(TestMetadataDB._HOST)
        mock_couch.Server().__contains__.assert_called_with(TestMetadataDB._DB)
        mock_couch.Server().create.assert_called_with(TestMetadataDB._DB)
        mock_couch.Server().__getitem__.assert_called_with(TestMetadataDB._DB)

        # Connect to existing
        mock_couch.reset_mock()
        mock_couch.Server().__contains__.return_value = True
        _ = MetadataDB(TestMetadataDB._DB, TestMetadataDB._HOST)

        mock_couch.Server().create.assert_not_called()

if __name__ == '__main__':
    unittest.main()
