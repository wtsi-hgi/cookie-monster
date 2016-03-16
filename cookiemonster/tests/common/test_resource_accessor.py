import os
import tempfile
import unittest
from os.path import dirname, realpath

from os.path import normpath, join

from cookiemonster.tests.common.stubs import StubResourceRequiringRegisteringDataSource, StubResourceAccessor, \
    StubResourceAccessorContainer

EXAMPLE_RESOURCE_ACCESSOR_CONTAINER_FILE_LOCATION = normpath(join(dirname(realpath(__file__)),
                                                                  "_example_resource_accessor_container.py"))

class TestResourceRequiringRegisteringDataSource(unittest.TestCase):
    """
    Tests for `ResourceRequiringRegisteringDataSource`.
    """
    def setUp(self):
        self._temp_directory = tempfile.mkdtemp()
        self._resource_accessor = StubResourceAccessor()
        self._data_source = StubResourceRequiringRegisteringDataSource(
            self._temp_directory, StubResourceAccessorContainer, self._resource_accessor)

    def test_extract_data_from_file(self):
        self._data_source.start()
        # Don't have to wait to start
        extracted = self._data_source.extract_data_from_file(EXAMPLE_RESOURCE_ACCESSOR_CONTAINER_FILE_LOCATION)
        self.assertEqual(len(list(extracted)), 3)

        for resource_accessor_container in extracted:
            self.assertEqual(resource_accessor_container.resources_accessor, self._resource_accessor)

    def tearDown(self):
        os.rmdir(self._temp_directory)


if __name__ == "__main__":
    unittest.main()
