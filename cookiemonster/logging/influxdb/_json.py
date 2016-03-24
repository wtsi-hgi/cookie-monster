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
from hgijson.json.builders import MappingJSONEncoderClassBuilder
from hgijson.json.models import JsonPropertyMapping
from hgijson.json.primitive import DatetimeISOFormatJSONEncoder, \
    DatetimeISOFormatJSONDecoder

from cookiemonster.logging.influxdb.models import InfluxDBLog

_INFLUX_DB_MEASUREMENT_PROPERTY = "measurement"
_INFLUX_DB_FIELDS_PROPERTY = "fields"
_INFLUX_DB_VALUE_PROPERTY = "value"
_INFLUX_DB_TIMESTAMP_PROPERTY = "time"
_INFLUX_DB_TAGS_PROPERTY = "tags"


_influxdb_log_json_encoder_mappings = [
    JsonPropertyMapping(_INFLUX_DB_MEASUREMENT_PROPERTY, "measured", "measured"),
    JsonPropertyMapping(json_property_getter=lambda field_as_json: field_as_json[_INFLUX_DB_FIELDS_PROPERTY],
                        json_property_setter=lambda log_as_json, values: log_as_json.update({_INFLUX_DB_FIELDS_PROPERTY: values}),
                        object_property_name="values"),
    JsonPropertyMapping(_INFLUX_DB_TIMESTAMP_PROPERTY, "timestamp",
                        encoder_cls=DatetimeISOFormatJSONEncoder, decoder_cls=DatetimeISOFormatJSONDecoder),
    JsonPropertyMapping(_INFLUX_DB_TAGS_PROPERTY, "metadata", "metadata", optional=True)
]
InfluxDBLogJSONEncoder = MappingJSONEncoderClassBuilder(InfluxDBLog, _influxdb_log_json_encoder_mappings).build()
