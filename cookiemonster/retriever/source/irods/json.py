"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
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
