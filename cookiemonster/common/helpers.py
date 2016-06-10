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
from datetime import datetime
from typing import List, Optional, Union, Sequence

import pytz

from cookiemonster.common.models import EnrichmentDiff, Enrichment
from hgicommon.collections import Metadata
from hgijson import DatetimeEpochJSONEncoder, DatetimeEpochJSONDecoder, JsonPropertyMapping, \
    MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder


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


def get_enrichment_changes_from_source(enrichments: Sequence[Enrichment], source: str,
                                       keys: Union[None, str, List[str]]=None,
                                       since: Optional[datetime]=None) -> List[EnrichmentDiff]:
    """
    Get the running changes in metadata from an enrichment source
    and, optional, key/list of keys based from the first known
    enrichment

    @param   enrichments    Enrichments ordered by timestamp (most recent last)
    @param   source  Enrichment source
    @param   keys    Metadata key(s) to check (optional; check all if omitted)
    @param   since   Time from which to check for changes (optional; check all if omitted)
    @return  List of differences

    TODO? Do we need a `before` parameter, as well...
    """
    first_comparator_index = 1
    output = []

    if since:
        for enrichment in enrichments[1:]:
            if enrichment.timestamp < since:
                first_comparator_index += 1
            else:
                # Enrichment list should be ordered by timestamp
                break

    total = len(enrichments)
    if total <= first_comparator_index:
        return output

    # Normalise single key
    if isinstance(keys, str):
        keys = [keys]

    for i in range(first_comparator_index, total):
        diff = EnrichmentDiff(enrichments[i - 1], enrichments[i], keys)
        if diff.is_different():
            output.append(diff)

    return output


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
