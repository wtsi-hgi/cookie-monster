from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder
from hgijson.json.models import JsonPropertyMapping

from cookiemonster.logging.influxdb.models import InfluxDBLog


_INFLUX_DB_MEASUREMENT_PROPERTY = "measurement"
_INFLUX_DB_TAGS_PROPERTY = "tags"
_INFLUX_DB_FIELDS_PROPERTY = "fields"
_INFLUX_DB_VALUE_PROPERTY = "value"
_INFLUX_DB_TIMESTAMP_PROPERTY = "timestamp"


influxdb_log_json_mappings = [
    JsonPropertyMapping(_INFLUX_DB_MEASUREMENT_PROPERTY, "measurement_name", "measurement_name"),
    JsonPropertyMapping(_INFLUX_DB_TAGS_PROPERTY, "metadata", "metadata"),
    JsonPropertyMapping(json_property_getter=lambda field_as_json: field_as_json[_INFLUX_DB_VALUE_PROPERTY],
                        json_property_setter=lambda log_as_json, value: log_as_json.update({_INFLUX_DB_VALUE_PROPERTY: value}),
                        object_property_name="metadata"),
    JsonPropertyMapping(_INFLUX_DB_TIMESTAMP_PROPERTY, "timestamp"),
]
InfluxDBLogJSONEncoder = MappingJSONEncoderClassBuilder(InfluxDBLog, influxdb_log_json_mappings).build()
InfluxDBLogJSONDecoder = MappingJSONDecoderClassBuilder(InfluxDBLog, influxdb_log_json_mappings).build()
