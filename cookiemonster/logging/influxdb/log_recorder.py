from json import JSONEncoder
from typing import Dict

from influxdb import InfluxDBClient

from cookiemonster.logging.influxdb._json import InfluxDBLogJSONEncoder
from cookiemonster.logging.influxdb.models import InfluxDBConnectionConfig, InfluxDBLog
from cookiemonster.logging.log_recorder import LogRecorder
from cookiemonster.logging.models import Log


class InfluxDBLogRecorder(LogRecorder):
    """
    Log recorder for InfluxDB.
    """
    _INFLUXDB_LOG_JSON_ENCODER = InfluxDBLogJSONEncoder()   # type: JSONEncoder

    def __init__(self, connection_config: InfluxDBConnectionConfig, static_tags: Dict=None):
        """
        constructor.
        :param connection_config: connection configuration for the InfluxDB database
        :param static_tags: tags for all logs
        """
        self._client = InfluxDBClient(connection_config)
        self._static_tags = static_tags if static_tags is not None else dict()

    def log(self, log: Log):
        influxdb_log = InfluxDBLog.value_of(log)    # type: InfluxDBLog
        influxdb_log.metadata.update(self._static_tags)

        influxdb_Log_as_json = InfluxDBLogRecorder._INFLUXDB_LOG_JSON_ENCODER.default(influxdb_log)

        self._client.write_points(influxdb_Log_as_json)
