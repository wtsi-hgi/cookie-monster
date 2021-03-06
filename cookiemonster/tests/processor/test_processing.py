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
from unittest.mock import MagicMock

from cookiemonster.common.models import Cookie
from cookiemonster.tests.processor._stubs import StubProcessor


class TestProcessor(unittest.TestCase):
    """
    Tests for `Processor`.
    """
    def setUp(self):
        self._cookie = Cookie("id")
        self._processor = StubProcessor()
        self._processor.evaluate_rules_with_cookie = MagicMock()
        self._processor.handle_cookie_enrichment = MagicMock()

    def test_process_cookie_when_no_termination(self):
        self._processor.evaluate_rules_with_cookie.return_value = False
        self._processor.process_cookie(self._cookie)

        self._processor.evaluate_rules_with_cookie.assert_called_once_with(self._cookie)
        self._processor.handle_cookie_enrichment.assert_called_once_with(self._cookie)

    def test_process_cookie_when_termination(self):
        self._processor.evaluate_rules_with_cookie.return_value = True
        self._processor.process_cookie(self._cookie)

        self._processor.evaluate_rules_with_cookie.assert_called_once_with(self._cookie)
        self._processor.handle_cookie_enrichment.assert_not_called()


if __name__ == "__main__":
    unittest.main()
