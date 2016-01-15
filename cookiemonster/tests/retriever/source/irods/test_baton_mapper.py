import unittest
from datetime import datetime
from os.path import dirname, normpath, realpath, join

import math
from hgicommon.collections import Metadata
from testwithbaton.api import TestWithBatonSetup
from testwithbaton.helpers import SetupHelper

from cookiemonster.retriever.source.irods._constants import METADATA_UPDATES_QUERY_ALIAS
from cookiemonster.retriever.source.irods.baton_mapper import BatonUpdateMapper, DATA_UPDATES_QUERY_ALIAS
from cookiemonster.tests.retriever.source.irods._settings import BATON_DOCKER_BUILD

REQUIRED_SPECIFIC_QUERIES = {
    DATA_UPDATES_QUERY_ALIAS: join("resources", "specific-queries", "data-updates.sql"),
    METADATA_UPDATES_QUERY_ALIAS: join("resources", "specific-queries", "metadata-updates.sql")
}

_DATA_OBJECT_NAMES = ["data_object_1", "data_object_2"]
_METADATA_KEYS = ["key_1", "key_2"]
_METADATA_VALUES = ["value_1", "value_2"]

_MAX_IRODS_TIMESTAMP = int(math.pow(2, 31)) - 1


class TestBatonUpdateMapper(unittest.TestCase):
    """
    Tests for `BatonUpdateMapper`.
    """
    def setUp(self):
        self.test_with_baton = TestWithBatonSetup(baton_docker_build=BATON_DOCKER_BUILD)
        self.test_with_baton.setup()
        self.setup_helper = SetupHelper(self.test_with_baton.icommands_location)

        self._install_update_queries()

        self.mapper = BatonUpdateMapper(
                self.test_with_baton.baton_location, self.test_with_baton.irods_test_server.users[0])

    def test_get_all_since_with_date_in_future(self):
        updates = self.mapper.get_all_since(datetime.fromtimestamp(_MAX_IRODS_TIMESTAMP))
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

    def test_get_all_since_with_updates_to_data_object_replica(self):
        inital_updates = self.mapper.get_all_since(datetime.min)
        location = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])

        resource_name = self.setup_helper.create_replica_storage()
        self.setup_helper.replicate_data_object(location, resource_name)

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)

        self.assertEquals(len(updates), 1)
        self.assertEquals(len(updates.get_entity_updates(location)), 1)

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
        # (see discussion: https://github.com/wtsi-hgi/cookie-monster/issues/3)
        self.assertEquals(len(relevant_updates), 1)
        self.assertEquals(relevant_updates[0].target, location)
        self.assertEquals(relevant_updates[0].metadata, metadata)

    def tearDown(self):
        self.test_with_baton.tear_down()

    def _install_update_queries(self):
        """
        Installs the specific queries required to get file updates from iRODS.
        """
        for alias, query_location_relative_to_root in REQUIRED_SPECIFIC_QUERIES.items():
            query_location = normpath(join(dirname(realpath(__file__)), "..", "..", "..", "..", "..",
                                           query_location_relative_to_root))
            with open(query_location) as file:
                query = file.read().replace('\n', ' ')

            self.setup_helper.run_icommand("iadmin", ["asq", "\"%s\"" % query, alias])


if __name__ == "__main__":
    unittest.main()
