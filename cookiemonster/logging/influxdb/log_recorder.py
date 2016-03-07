from json import JSONEncoder
from time import sleep
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
        Constructor.
        :param connection_config: connection configuration for the InfluxDB database
        :param static_tags: tags to put on all logs
        """
        self.static_tags = static_tags if static_tags is not None else dict()
        self._influxdb_client = InfluxDBClient(connection_config.host, connection_config.port, connection_config.user,
                                               connection_config.password, connection_config.database)

    def log(self, log: Log):
        influxdb_log = InfluxDBLog.value_of(log)    # type: InfluxDBLog
        influxdb_log.metadata.update(self.static_tags)

        influxdb_log_as_json = InfluxDBLogRecorder._INFLUXDB_LOG_JSON_ENCODER.default(influxdb_log)

        self._influxdb_client.write_points([influxdb_log_as_json])
