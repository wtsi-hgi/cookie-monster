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
import json
import unittest

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
