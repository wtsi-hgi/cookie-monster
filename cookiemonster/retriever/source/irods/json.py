from baton.json import DataObjectReplicaCollectionJSONDecoder, IrodsMetadataJSONDecoder, IrodsMetadataJSONEncoder
from baton.json import DataObjectReplicaCollectionJSONEncoder
from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder
from hgijson.json.models import JsonPropertyMapping

from cookiemonster.retriever.source.irods.models import DataObjectModification, IrodsEntityModification

_irods_entity_modification_json_mappings = [
    JsonPropertyMapping("modified_metadata", "modified_metadata", "modified_metadata",
                        encoder_cls=IrodsMetadataJSONEncoder, decoder_cls=IrodsMetadataJSONDecoder)
]
IrodsEntityModificationJSONEncoder = MappingJSONEncoderClassBuilder(
    IrodsEntityModification, _irods_entity_modification_json_mappings).build()
IrodsEntityModificationJSONDecoder = MappingJSONDecoderClassBuilder(
    IrodsEntityModification, _irods_entity_modification_json_mappings).build()


_data_object_modification_json_mappings = [
    JsonPropertyMapping("modified_replicas", "modified_replicas", "modified_replicas",
                        encoder_cls=DataObjectReplicaCollectionJSONEncoder,
                        decoder_cls=DataObjectReplicaCollectionJSONDecoder)
]
DataObjectModificationJSONEncoder = MappingJSONEncoderClassBuilder(
    DataObjectModification, _data_object_modification_json_mappings, (IrodsEntityModificationJSONEncoder, )).build()
DataObjectModificationJSONDecoder = MappingJSONDecoderClassBuilder(
    DataObjectModification, _data_object_modification_json_mappings, (IrodsEntityModificationJSONDecoder, )).build()
