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
from dateutil.parser import parser
from baton.json import DataObjectReplicaCollectionJSONDecoder, IrodsMetadataJSONDecoder, IrodsMetadataJSONEncoder
from baton.json import DataObjectReplicaCollectionJSONEncoder
from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder
from hgijson.json.models import JsonPropertyMapping

from cookiemonster.retriever.source.irods.models import IrodsEntityUpdate, DataObjectModification

_irods_entity_modification_json_mappings = [
    JsonPropertyMapping("modified_metadata", "modified_metadata", "modified_metadata",
                        encoder_cls=IrodsMetadataJSONEncoder, decoder_cls=IrodsMetadataJSONDecoder)
]
_IrodsEntityModificationJSONEncoder = MappingJSONEncoderClassBuilder(
    IrodsEntityUpdate, _irods_entity_modification_json_mappings).build()
_IrodsEntityModificationJSONDecoder = MappingJSONDecoderClassBuilder(
    IrodsEntityUpdate, _irods_entity_modification_json_mappings).build()


_data_object_modification_json_mappings = [
    JsonPropertyMapping("modified_replicas", "modified_replicas", "modified_replicas",
                        encoder_cls=DataObjectReplicaCollectionJSONEncoder,
                        decoder_cls=DataObjectReplicaCollectionJSONDecoder)
]
DataObjectModificationJSONEncoder = MappingJSONEncoderClassBuilder(
    DataObjectModification, _data_object_modification_json_mappings, (_IrodsEntityModificationJSONEncoder,)).build()
DataObjectModificationJSONDecoder = MappingJSONDecoderClassBuilder(
    DataObjectModification, _data_object_modification_json_mappings, (_IrodsEntityModificationJSONDecoder,)).build()
