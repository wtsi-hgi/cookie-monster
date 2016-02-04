import json
import unittest

from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from baton.models import DataObjectReplica

from cookiemonster.retriever.source.irods.json import DataObjectModificationJSONEncoder, \
    DataObjectModificationJSONDecoder
from cookiemonster.retriever.source.irods.models import DataObjectModification

_data_object_modification = None
_data_object_modification_as_json = None


# def _create_data_object_modification_with_json_representation() -> Tuple[DataObjectModification, Dict]:
#     """
#     Gets an example of a `DataObjectModification` from baton, along with its JSON representation.
#     """
#     global _data_object_modification, _data_object_modification_as_json
#
#     # Starting baton is expensive - get view of baton JSON and cache
#     if _data_object_modification is None:
#         test_with_baton = TestWithBatonSetup(baton_docker_build=BATON_DOCKER_BUILD)
#         test_with_baton.setup()
#         setup_helper = SetupHelper(test_with_baton.icommands_location)
#         install_queries(REQUIRED_SPECIFIC_QUERIES, setup_helper)
#
#         # "Cheating" using the update mapper to get the time needed to exclude previous updates
#         mapper = BatonUpdateMapper(test_with_baton.baton_location, test_with_baton.irods_test_server.users[0].zone)
#         start_datetime = mapper.get_all_since(datetime.min).get_most_recent()[0].timestamp
#         start_unix_time = int(start_datetime.timestamp())
#         end_unix_time = 2147483647
#
#         location = setup_helper.create_data_object("name_1")
#
#         for query_alias in [MODIFIED_DATA_QUERY_ALIAS, MODIFIED_METADATA_QUERY_ALIAS]:
#             baton_runner = BatonRunner(test_with_baton.baton_location, test_with_baton.irods_test_server.users[0].zone)
#             specific_query_json = baton_runner.run_baton_query(
#                 BatonBinary.BATON,
#                 ["-s", MODIFIED_DATA_QUERY_ALIAS, "-b", str(start_unix_time), "-b", str(end_unix_time)])
#             query = baton_runner.run_baton_query(BatonBinary.BATON_SPECIFIC_QUERY, input_data=specific_query_json)
#
#         print(query)
#
#
#
#         # JSON representation not "standardised" completely by baton
#         # _data_object_modification_as_json = {
#         #     "modified_metadata": MetadataJSONEncoder().default(updates[0].metadata["modified_metadata"]),
#         #     "modified_replicas": DataObjectReplicaCollectionJSONEncoder().default(
#         #         updates[0].metadata["modified_replicas"])
#         # }
#
#         exit()
#
#         _data_object_modification = DataObjectModification()
#         _data_object_modification.modified_replicas.add(DataObjectReplica(0, "", up_to_date=True))
#
#     return deepcopy(_data_object_modification), deepcopy(_data_object_modification_as_json)


class TestDataObjectModificationJSONEncoderAndDecoder(unittest.TestCase):
    """
    Tests for `DataObjectModificationJSONEncoder` and `DataObjectModificationJSONDecoder`.
    """
    def setUp(self):
        self.data_object_modification = DataObjectModification()
        self.data_object_modification.modified_metadata = IrodsMetadata({"key_1": {"value_1"}, "key_2": {"value_2"}})
        self.data_object_modification.modified_replicas = DataObjectReplicaCollection([
            DataObjectReplica(0, "checksum_1", "host_1", "resource_1", True),
            DataObjectReplica(1, "checksum_2", "host_2", "resource_2", False)
        ])

    def test_encode_then_decode(self):
        encoded = DataObjectModificationJSONEncoder().default(self.data_object_modification)
        decoded = DataObjectModificationJSONDecoder().decode(json.dumps(encoded))
        self.assertEqual(decoded, self.data_object_modification)

    def test_encoder_with_json_dumps(self):
        encoded = json.dumps(self.data_object_modification, cls=DataObjectModificationJSONEncoder)
        decoded = DataObjectModificationJSONDecoder().decode(encoded)
        self.assertEqual(decoded, self.data_object_modification)

    def test_decoder_with_json_loads(self):
        encoded = DataObjectModificationJSONEncoder().default(self.data_object_modification)
        decoded = json.loads(json.dumps(encoded), cls=DataObjectModificationJSONDecoder)
        self.assertEqual(decoded, self.data_object_modification)


if __name__ == "__main__":
    unittest.main()
