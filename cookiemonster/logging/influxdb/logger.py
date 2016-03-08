import atexit
import logging
from datetime import timedelta
from json import JSONEncoder
from threading import Lock, Timer
from typing import Dict, Optional, List

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
                 buffer_latency: Optional[timedelta]=timedelta(seconds=10)):
        """
        Constructor.
        :param connection_config: connection configuration for the InfluxDB database
        :param static_tags: tags to put on all logs
        :param buffer_latency: the time in which the log buffer is filled before logs are written. Set to `None` if
        logs should be written straight away.
        """
        self.static_tags = static_tags if static_tags is not None else dict()
        self.buffer_latency = buffer_latency

        self._influxdb_client = InfluxDBClient(connection_config.host, connection_config.port, connection_config.user,
                                               connection_config.password, connection_config.database)
        self._buffer = []    # type: List[Log]
        self._buffer_timer = None   # type: Optional[Timer]
        self._buffer_lock = Lock()

        # Flush the buffer on exit
        atexit.register(self.flush)

    def flush(self):
        """
        Flush any logs from the buffer.
        """
        with self._buffer_lock:
            influxdb_logs_as_json_list = []     # type: List[Dict]
            for log in self._buffer:
                influxdb_logs_as_json_list.append(InfluxDBLogger._convert_log_to_json(log))
            self._buffer.clear()

            if self._buffer_timer is not None and self._buffer_timer.is_alive():
                self._buffer_timer.cancel()
            self._buffer_timer = None

        logging.debug("Logging: %s" % influxdb_logs_as_json_list)
        if len(influxdb_logs_as_json_list) > 0:
            successful = self._influxdb_client.write_points(influxdb_logs_as_json_list)
            if not successful:
                logging.error("Error when sending logs to InfluxDB (%d logs were lost)"
                              % len(influxdb_logs_as_json_list))
            else:
                logging.info("%d logs successfully sent to InfluxDB" % len(influxdb_logs_as_json_list))

    def _process_log(self, log: Log):
        log.metadata.update(self.static_tags)

        with self._buffer_lock:
            self._buffer.append(log)
            if self._buffer_timer is None and self.buffer_latency is not None:
                self._buffer_timer = Timer(self.buffer_latency.total_seconds(), self.flush)
                self._buffer_timer.start()

        if self.buffer_latency is None or self.buffer_latency.total_seconds() == 0:
            self.flush()

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
