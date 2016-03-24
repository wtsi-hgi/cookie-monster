"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

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
import os
import tempfile
import unittest
from os.path import dirname, realpath

from os.path import normpath, join

from cookiemonster.tests.common.stubs import StubResourceAccessorContainerRegisteringDataSource, StubResourceAccessor, \
    StubResourceAccessorContainer

EXAMPLE_RESOURCE_ACCESSOR_CONTAINER_FILE_LOCATION = normpath(join(dirname(realpath(__file__)),
                                                                  "_example_resource_accessor_container.py"))

class TestResourceAccessorContainerRegisteringDataSource(unittest.TestCase):
    """
    Tests for `ResourceAccessorContainerRegisteringDataSource`.
    """
    def setUp(self):
        self._temp_directory = tempfile.mkdtemp()
        self._resource_accessor = StubResourceAccessor()
        self._data_source = StubResourceAccessorContainerRegisteringDataSource(
            self._temp_directory, StubResourceAccessorContainer, self._resource_accessor)

    def test_extract_data_from_file(self):
        self._data_source.start()
        # Don't have to wait to start
        extracted = self._data_source.extract_data_from_file(EXAMPLE_RESOURCE_ACCESSOR_CONTAINER_FILE_LOCATION)
        self.assertEqual(len(list(extracted)), 3)

        for resource_accessor_container in extracted:
            self.assertEqual(resource_accessor_container.resource_accessor, self._resource_accessor)

    def tearDown(self):
        os.rmdir(self._temp_directory)


if __name__ == "__main__":
    unittest.main()
