import math
import unittest
from datetime import datetime
from os.path import join

from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from baton.models import DataObjectReplica
from hgicommon.collections import Metadata
from testwithbaton.api import TestWithBatonSetup
from testwithbaton.helpers import SetupHelper

from cookiemonster.retriever.source.irods._constants import MODIFIED_METADATA_QUERY_ALIAS
from cookiemonster.retriever.source.irods.baton_mapper import BatonUpdateMapper, MODIFIED_DATA_QUERY_ALIAS
from cookiemonster.retriever.source.irods.json import DataObjectModificationJSONEncoder
from cookiemonster.retriever.source.irods.models import DataObjectModification
from cookiemonster.tests.retriever.source.irods._helpers import install_queries
from cookiemonster.tests.retriever.source.irods._settings import BATON_DOCKER_BUILD

REQUIRED_SPECIFIC_QUERIES = {
    MODIFIED_DATA_QUERY_ALIAS: join("resources", "specific-queries", "data-modified-partial.sql"),
    MODIFIED_METADATA_QUERY_ALIAS: join("resources", "specific-queries", "metadata-modified-partial.sql")
}

_DATA_OBJECT_NAMES = ["data_object_1", "data_object_2"]
_METADATA_KEYS = ["key_1", "key_2"]
_METADATA_VALUES = ["value_1", "value_2", "value_2"]

_MAX_IRODS_TIMESTAMP = int(math.pow(2, 31)) - 1


class TestBatonUpdateMapper(unittest.TestCase):
    """
    Tests for `BatonUpdateMapper`.
    """
    def setUp(self):
        self.test_with_baton = TestWithBatonSetup(baton_docker_build=BATON_DOCKER_BUILD)
        self.test_with_baton.setup()
        self.setup_helper = SetupHelper(self.test_with_baton.icommands_location)
        install_queries(REQUIRED_SPECIFIC_QUERIES, self.setup_helper)

        self.mapper = BatonUpdateMapper(
                self.test_with_baton.baton_location, self.test_with_baton.irods_server.users[0])

    def test_get_all_since_with_date_in_future(self):
        updates = self.mapper.get_all_since(datetime.fromtimestamp(_MAX_IRODS_TIMESTAMP))
        self.assertEqual(len(updates), 0)

    def test_get_all_since_with_date_in_past(self):
        inital_updates = self.mapper.get_all_since(datetime.min)

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)
        self.assertEqual(len(updates), 0)

    def test_get_all_since_with_data_object_updates(self):
        inital_updates = self.mapper.get_all_since(datetime.min)
        location_1 = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])
        location_2 = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[1])

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)
        self.assertEqual(len(updates), 2)
        self.assertEqual(len(updates.get_entity_updates(location_1)), 1)
        self.assertEqual(len(updates.get_entity_updates(location_2)), 1)
        # TODO: More detailed check on updates

    def test_get_all_since_with_updates_to_data_object_replica(self):
        inital_updates = self.mapper.get_all_since(datetime.min)
        location = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])
        resource = self.setup_helper.create_replica_storage()
        self.setup_helper.replicate_data_object(location, resource)
        self.setup_helper.update_checksums(location)

        checksum = self.setup_helper.get_checksum(location)
        replicas = DataObjectReplicaCollection([DataObjectReplica(i, checksum) for i in range(2)])
        expected_modification_description = DataObjectModification(modified_replicas=replicas)
        expected_metadata = Metadata(
                DataObjectModificationJSONEncoder().default(expected_modification_description))

        updates = self.mapper.get_all_since(inital_updates.get_most_recent()[0].timestamp)
        self.assertEquals(len(updates), 1)
        self.assertIn(updates[0].target, location)
        self.assertCountEqual(updates[0].metadata, expected_metadata)

    def test_get_all_since_with_metadata_update(self):
        location = self.setup_helper.create_data_object(_DATA_OBJECT_NAMES[0])
        updates_before_metadata_added = self.mapper.get_all_since(datetime.min)

        metadata_1 = Metadata({
            _METADATA_KEYS[0]: _METADATA_VALUES[0],
            _METADATA_KEYS[1]: _METADATA_VALUES[1]
        })
        self.setup_helper.add_metadata_to(location, metadata_1)
        # Update pre-existing metadata item
        metadata_2 = Metadata({_METADATA_KEYS[0]: _METADATA_VALUES[2]})
        self.setup_helper.add_metadata_to(location, metadata_2)
        expected_irods_metadata = IrodsMetadata({
            _METADATA_KEYS[0]: {_METADATA_VALUES[0], _METADATA_VALUES[2]},
            _METADATA_KEYS[1]: {_METADATA_VALUES[1]}
        })

        modification_description = DataObjectModification(expected_irods_metadata)
        expected_update_metadata = Metadata(DataObjectModificationJSONEncoder().default(modification_description))

        updates = self.mapper.get_all_since(updates_before_metadata_added.get_most_recent()[0].timestamp)
        self.assertEqual(len(updates), 1)
        relevant_updates = updates.get_entity_updates(location)
        # Expect the mapper to have combined all updates into one (https://github.com/wtsi-hgi/cookie-monster/issues/3)
        self.assertEqual(len(relevant_updates), 1)
        self.assertEqual(relevant_updates[0].target, location)
        self.assertEqual(relevant_updates[0].metadata, expected_update_metadata)

    def tearDown(self):
        self.test_with_baton.tear_down()


if __name__ == "__main__":
    unittest.main()
