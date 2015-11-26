import unittest
from unittest import mock

from cookiemonster.cookiejar._dbi import DBI


class TestDBI(unittest.TestCase):
    _DB   = 'foo'
    _HOST = 'bar'

    @mock.patch('cookiemonster.cookiejar._dbi.couchdb')
    def test_metadata_init(self, mock_couch):
        # Connect and create
        mock_couch.Server().__contains__.return_value = False
        _ = DBI(TestDBI._DB, TestDBI._HOST)

        mock_couch.Server.assert_called_with(TestDBI._HOST)
        mock_couch.Server().__contains__.assert_called_with(TestDBI._DB)
        mock_couch.Server().create.assert_called_with(TestDBI._DB)
        mock_couch.Server().__getitem__.assert_called_with(TestDBI._DB)

        # Connect to existing
        mock_couch.reset_mock()
        mock_couch.Server().__contains__.return_value = True
        _ = DBI(TestDBI._DB, TestDBI._HOST)

        mock_couch.Server().create.assert_not_called()

if __name__ == '__main__':
    unittest.main()
