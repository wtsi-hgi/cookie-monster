import unittest
from datetime import datetime
from os.path import dirname, normpath, realpath, join
from time import sleep

from hgicommon.collections import Metadata
from testwithbaton.api import TestWithBatonSetup
from testwithbaton.helpers import SetupHelper

from cookiemonster.retriever.source.irods.baton_mapper import BatonUpdateMapper, SPECIFIC_QUERY_ALIAS
from cookiemonster.tests.retriever.source.irods._settings import BATON_DOCKER_BUILD

_SPECIFIC_QUERY_PATH_RELATIVE_TO_PROJECT_ROOT = join("resources", "specific-queries", "get-updates.sql")

_DATA_OBJECT_NAMES = ["data_object_1", "data_object_2"]
_COLLECTION_NAMES = ["collection_1", "collection_2"]
_METADATA_KEYS = ["key_1", "key_2"]
_METADATA_VALUES = ["value_1", "value_2"]


class TestBatonUpdateMapper(unittest.TestCase):
    """
    Tests for `BatonUpdateMapper`.
    """
    _query = None

    def setUp(self):
        self.test_with_baton = TestWithBatonSetup(baton_docker_build=BATON_DOCKER_BUILD)
        self.test_with_baton.setup()
        self.setup_helper = SetupHelper(self.test_with_baton.icommands_location)

        self._install_query()

        self.mapper = BatonUpdateMapper(
                self.test_with_baton.baton_location, self.test_with_baton.irods_test_server.users[0])

    def test_get_all_since_with_date_in_future(self):
        updates = self.mapper.get_all_since(datetime.max)
        self.assertEquals(len(updates), 0)

    def test_get_all_since_with_date_in_past(self):
        inital_updates = self.mapper.get_all_since(datetime.min)

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)
        self.assertEquals(len(updates), 0)

    def test_get_all_since_with_data_object_updates(self):
        inital_updates = self.mapper.get_all_since(datetime.min)
        location_1 = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])
        location_2 = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[1])

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)
        self.assertEquals(len(updates), 2)
        self.assertEquals(len(updates.get_entity_updates(location_1)), 1)
        self.assertEquals(len(updates.get_entity_updates(location_2)), 1)

    # XXX: No `test_get_all_since_with_collection_updates` as SQL query does not support collections

    def test_get_all_since_with_metadata_update(self):
        location = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])
        updates_before_metadata_added = self.mapper.get_all_since(datetime.min)

        metadata = Metadata({
            _METADATA_KEYS[0]: _METADATA_VALUES[0],
            _METADATA_KEYS[1]: _METADATA_VALUES[1]
        })
        self.setup_helper.add_metadata_to(location, metadata)

        updates = self.mapper.get_all_since(updates_before_metadata_added.get_most_recent()[0].timestamp)
        self.assertEquals(len(updates), 1)
        relevant_updates = updates.get_entity_updates(location)
        # Expect the mapper to have combined all updates into one
        # (see discussion: https://github.com/wtsi-hgi/cookie-monster/issues/3#issuecomment-168990482)
        self.assertEquals(len(relevant_updates), 1)
        self.assertEquals(relevant_updates[0].target, location)
        self.assertEquals(relevant_updates[0].metadata, metadata)

    def tearDown(self):
        self.test_with_baton.tear_down()

    def _install_query(self):
        """
        Installs the specific query required to get file updates from iRODS.
        """
        if TestBatonUpdateMapper._query is None:
            query_location = normpath(join(dirname(realpath(__file__)),
                                           "..", "..", "..", "..", "..", _SPECIFIC_QUERY_PATH_RELATIVE_TO_PROJECT_ROOT))
            with open(query_location) as file:
                TestBatonUpdateMapper._query = file.read().replace('\n', ' ')

        self.setup_helper.run_icommand("iadmin", ["asq", "\"%s\"" % TestBatonUpdateMapper._query, SPECIFIC_QUERY_ALIAS])


if __name__ == "__main__":
    unittest.main()