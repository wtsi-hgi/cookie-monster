import json
import unittest
from datetime import datetime

from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from baton.models import DataObjectReplica

from cookiemonster.retriever.source.irods.json import DataObjectModificationJSONEncoder, \
    DataObjectModificationJSONDecoder
from cookiemonster.retriever.source.irods.models import DataObjectModification

_data_object_modification = None
_data_object_modification_as_json = None


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
