"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
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
