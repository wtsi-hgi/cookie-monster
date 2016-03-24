"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
import datetime

import pytz

from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment

from hgijson.json.models import JsonPropertyMapping
from hgijson.json.primitive import DatetimeEpochJSONEncoder, DatetimeEpochJSONDecoder
from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder


def localise_to_utc(timestamp: datetime) -> datetime:
    """
    Localise the given timestamp to UTC.
    :param timestamp: the timestamp to localise
    :return: the timestamp localised to UTC
    """
    if timestamp.tzinfo is None:
        return pytz.utc.localize(timestamp)
    else:
        return timestamp.astimezone(pytz.utc)


_ENRICHMENT_JSON_MAPPING = [
    JsonPropertyMapping('source',    'source',
                                     object_constructor_parameter_name='source'),
    JsonPropertyMapping('timestamp', 'timestamp',
                                     object_constructor_parameter_name='timestamp',
                                     encoder_cls=DatetimeEpochJSONEncoder,
                                     decoder_cls=DatetimeEpochJSONDecoder),
    JsonPropertyMapping('metadata',  object_constructor_parameter_name='metadata',
                                     object_constructor_argument_modifier=Metadata,
                                     object_property_getter=lambda enrichment: dict(enrichment.metadata.items()))
]

EnrichmentJSONEncoder = MappingJSONEncoderClassBuilder(Enrichment, _ENRICHMENT_JSON_MAPPING).build()
EnrichmentJSONDecoder = MappingJSONDecoderClassBuilder(Enrichment, _ENRICHMENT_JSON_MAPPING).build()
