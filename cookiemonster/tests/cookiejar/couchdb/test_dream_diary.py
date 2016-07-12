"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

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
from unittest.mock import MagicMock

from cookiemonster.logging.injector import LoggingContext
from cookiemonster.cookiejar.couchdb import inject_logging
import cookiemonster.cookiejar.couchdb.dream_diary as _dd


def _mock_postexec(self, context:LoggingContext):
    """
    Override the postexec function of the logging function such that it
    always returns a duration of five seconds
    """
    self.log(context, 5)


class TestDreamDiary(unittest.TestCase):
    """
    Tests for CouchDB response time logging
    """
    def setUp(self):
        self._logger = MagicMock()

        self._db = MagicMock()
        self._db._db_methods = ['foo', 'bar', 'quux']

        # Fake methods
        for method in self._db._db_methods:
            setattr(self._db, method, MagicMock())
            setattr(getattr(self._db, method), '__name__', method)

        # Fake logging duration
        setattr(_dd._CouchDBResponseTimeLogging, 'postexec', _mock_postexec)

    def test_inject_logging(self):
        methods = self._db._db_methods

        # First check our functions aren't doing any logging...
        for method in methods:
            logged_fn = getattr(self._db, method)
            logged_fn()

            self.assertFalse(self._logger.called)

        # ...Then inject the logging...
        inject_logging(self._db, self._logger)

        # ...Now check that the logging is there
        for method in methods:
            logged_fn = getattr(self._db, method)
            logged_fn()

            log_name, log_args, log_kwargs = self._logger.method_calls[-1]
            self.assertEqual(log_name, 'record')
            self.assertEqual(log_args, ('couchdb', 5, {'function': method}))
            self.assertEqual(log_kwargs, {})


if __name__ == '__main__':
    unittest.main()
