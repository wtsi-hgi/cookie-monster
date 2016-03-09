import logging
from datetime import timedelta
from json import JSONEncoder
from typing import Dict, Iterable, List

from influxdb import InfluxDBClient

from cookiemonster.logging.influxdb._json import InfluxDBLogJSONEncoder
from cookiemonster.logging.influxdb.models import InfluxDBConnectionConfig, InfluxDBLog
from cookiemonster.logging.logger import Logger
from cookiemonster.logging.models import Log


class InfluxDBLogger(Logger):
    """
    Log recorder for InfluxDB.
    """
    _INFLUXDB_LOG_JSON_ENCODER = InfluxDBLogJSONEncoder()   # type: JSONEncoder

    def __init__(self, connection_config: InfluxDBConnectionConfig, static_tags: Dict=None,
                 buffer_latency: timedelta=None):
        """
        Constructor.
        :param connection_config: connection configuration for the InfluxDB database
        :param static_tags: tags to put on all logs
        :param buffer_latency: the time in which the log buffer is filled before logs are written. Set to `None` if
        logs should be written straight away.
        """
        super().__init__(buffer_latency)
        self.static_tags = static_tags if static_tags is not None else dict()

        self._influxdb_client = InfluxDBClient(connection_config.host, connection_config.port, connection_config.user,
                                               connection_config.password, connection_config.database)

    def _write_logs(self, logs: Iterable[Log]):
        influxdb_logs_as_json = []     # type: List[Dict]
        for log in logs:
            log.metadata.update(self.static_tags)
            influxdb_log_as_json = InfluxDBLogger._convert_log_to_json(log)
            influxdb_logs_as_json.append(influxdb_log_as_json)

        logging.debug("Writing logs: %s" % influxdb_logs_as_json)
        successful = self._influxdb_client.write_points(influxdb_logs_as_json)
        if not successful:
            logging.error("Error when sending logs to InfluxDB (%d logs were lost)" % len(influxdb_logs_as_json))
        else:
            logging.info("%d log(s) sent successfully to InfluxDB" % len(influxdb_logs_as_json))

    @staticmethod
    def _convert_log_to_json(log: Log) -> Dict:
        """
        Converts the given log to the JSON representation used by the InfluxDB client.
        :param log: the log to convert
        :return: the log as JSON
        """
        influxdb_log = InfluxDBLog.value_of(log)    # type: InfluxDBLog
        influxdb_log_as_json = InfluxDBLogger._INFLUXDB_LOG_JSON_ENCODER.default(influxdb_log)
        return influxdb_log_as_json
