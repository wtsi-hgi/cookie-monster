"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

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
