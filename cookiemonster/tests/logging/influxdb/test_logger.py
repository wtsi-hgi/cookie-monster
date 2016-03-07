import json
import unittest
from datetime import datetime, timezone

from hgijson.json.primitive import DatetimeISOFormatJSONDecoder
from influxdb import InfluxDBClient

from cookiemonster.logging.influxdb.logger import InfluxDBLogger
from cookiemonster.logging.influxdb.models import InfluxDBConnectionConfig
from cookiemonster.logging.models import Log
from cookiemonster.tests.logging.influxdb._helpers import setup_influxdb_in_docker

_INFLUXDB_DOCKERHUB_REPOSITORY = "tutum/influxdb"
_INFLUXDB_VERSION = "0.10"
_INFLUXDB_USER = "root"
_INFLUXDB_PASSWORD = "root"
_INFLUXDB_DATABASE = "testDB"


class TestInfluxDBLoggger(unittest.TestCase):
    """
    Tests for `InfluxDBLogger`.
    """
    def setUp(self):
        host, http_api_port, self._tear_down = setup_influxdb_in_docker(
            _INFLUXDB_DOCKERHUB_REPOSITORY, _INFLUXDB_VERSION)

        self._influxdb_client = InfluxDBClient(host, http_api_port, _INFLUXDB_USER, _INFLUXDB_PASSWORD)
        self._influxdb_client.create_database(_INFLUXDB_DATABASE)

        connection_config = InfluxDBConnectionConfig(
            host, http_api_port, _INFLUXDB_USER, _INFLUXDB_PASSWORD, _INFLUXDB_DATABASE)

        self._logger = InfluxDBLogger(connection_config)

    def test_record(self):
        log = Log("measuring", 123, {"host": "1"}, datetime(2015, 3, 2, tzinfo=timezone.utc))
        self._logger.record(log.measuring, log.value, log.metadata, log.timestamp)
        self._logger.record("measuring", 456)

        retrieved = self._influxdb_client.query("select * from measuring where host = '1'", database=_INFLUXDB_DATABASE)
        retrieved_point = list(retrieved.get_points())[0]

        self.assertEqual(retrieved_point["host"], log.metadata["host"])
        self.assertEqual(json.loads(retrieved_point["time"], cls=DatetimeISOFormatJSONDecoder), log.timestamp)
        self.assertEqual(retrieved_point["value"], log.value)

    def test_record_when_static_tags(self):
        self._logger.static_tags = {"host": "1"}

        log = Log("measuring", 123)
        self._logger.record(log.measuring, log.value)

        retrieved = self._influxdb_client.query("select * from measuring where host = '1'", database=_INFLUXDB_DATABASE)
        retrieved_point = list(retrieved.get_points())[0]

        self.assertEqual(retrieved_point["value"], log.value)

    def tearDown(self):
        self._tear_down()


if __name__ == "__main__":
    unittest.main()
