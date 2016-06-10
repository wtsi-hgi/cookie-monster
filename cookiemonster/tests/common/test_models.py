"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

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

from cookiemonster.common.models import Cookie, Enrichment
from hgicommon.collections import Metadata


class TestCookie(unittest.TestCase):
    """
    Tests for `Cookie`.
    """
    _IDENTIFIER = "id"

    def setUp(self):
        self._cookie = Cookie(TestCookie._IDENTIFIER)

    def test_enrich(self):
        enrichment = Enrichment("source", datetime(1, 1, 1), Metadata())
        self._cookie.enrich(enrichment)
        self.assertCountEqual(self._cookie.enrichments, [enrichment])


if __name__ == "__main__":
    unittest.main()
