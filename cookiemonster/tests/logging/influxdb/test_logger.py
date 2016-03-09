import json
import unittest
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from hgijson.json.primitive import DatetimeISOFormatJSONDecoder
from influxdb import InfluxDBClient

from cookiemonster.logging.influxdb.logger import InfluxDBLogger
from cookiemonster.logging.influxdb.models import InfluxDBConnectionConfig
from cookiemonster.logging.models import Log
from cookiemonster.tests.logging.influxdb._helpers import setup_influxdb_in_docker

_INFLUXDB_DOCKERHUB_REPOSITORY = "tutum/influxdb"
_INFLUXDB_VERSION = "0.9"
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

        self._logger = InfluxDBLogger(connection_config, buffer_latency=None)

    def tearDown(self):
        self._tear_down()

    def _get_all_points(self) -> List[Dict]:
        """
        Gets all points within all tables.
        :return:
        """
        retrieved = self._influxdb_client.query("select * from /.*/", database=_INFLUXDB_DATABASE)
        return list(retrieved.get_points())

    def test_record_value(self):
        log = Log("measured", 123, {"host": "1"}, datetime(2015, 3, 2, tzinfo=timezone.utc))
        self._logger.record(log.measured, log.values, log.metadata, log.timestamp)
        self._logger.record("measured", 456)

        retrieved = self._influxdb_client.query("select * from measured where host = '1'", database=_INFLUXDB_DATABASE)
        self.assertEqual(len(list(retrieved.get_points())), 1)
        retrieved_point = list(retrieved.get_points())[0]

        self.assertEqual(retrieved_point["host"], log.metadata["host"])
        self.assertEqual(json.loads(retrieved_point["time"], cls=DatetimeISOFormatJSONDecoder), log.timestamp)
        self.assertEqual(retrieved_point["value"], log.value)

    def test_record_named_values(self):
        values = {
            "a": 1,
            "b": 2
        }
        log = Log("measured", values, timestamp=datetime(2015, 3, 2, tzinfo=timezone.utc))
        self._logger.record(log.measured, log.values, log.metadata, log.timestamp)

        points = self._get_all_points()
        self.assertEqual(len(points), 1)

        self.assertEqual(points[0]["a"], log.values["a"])
        self.assertEqual(points[0]["b"], log.values["b"])

    def test_record_when_static_tags(self):
        self._logger.static_tags = {"host": "1"}

        log = Log("measured", 123)
        self._logger.record(log.measured, log.values)

        retrieved = self._influxdb_client.query("select * from measured where host = '1'", database=_INFLUXDB_DATABASE)
        retrieved_point = list(retrieved.get_points())[0]

        self.assertEqual(retrieved_point["value"], log.value)

    def test_flush_with_empty_buffer(self):
        self._logger.flush()
        self.assertEqual(len(self._get_all_points()), 0)

    def test_flush_with_buffer(self):
        self._logger.buffer_latency_in_seconds = timedelta(days=999).total_seconds()

        self._logger.record("measured", 123, timestamp=datetime(2016, 1, 1))
        self.assertEqual(len(self._get_all_points()), 0)
        self._logger.flush()
        self.assertEqual(len(self._get_all_points()), 1)
        self._logger.flush()
        self.assertEqual(len(self._get_all_points()), 1)
        self._logger.record("measured", 456, timestamp=datetime(2016, 1, 10))
        self.assertEqual(len(self._get_all_points()), 1)
        self._logger.flush()
        self.assertEqual(len(self._get_all_points()), 2)


if __name__ == "__main__":
    unittest.main()
