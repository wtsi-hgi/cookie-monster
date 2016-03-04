import logging
import unittest
from datetime import datetime
from time import sleep
from urllib.parse import urlparse

from influxdb import InfluxDBClient

from cookiemonster.logging.influxdb._json import InfluxDBLogJSONDecoder
from cookiemonster.logging.influxdb.log_recorder import InfluxDBLogRecorder
from cookiemonster.logging.influxdb.models import InfluxDBConnectionConfig
from cookiemonster.logging.models import Log
from cookiemonster.tests._utils.docker_couchdb import _get_port
from docker import Client
from docker.utils import kwargs_from_env

INFLUXDB_DOCKERHUB_REPOSITORY = "tutum/influxdb"
INFLUXDB_VERSION = "0.10"


class TestInfluxDBLogRecoder(unittest.TestCase):
    """
    Tests for `InfluxDBLogRecoder`.
    """
    def setUp(self):
        # logging.root.setLevel(logging.DEBUG)

        docker_environment = kwargs_from_env(assert_hostname=False)
        self._docker_client = Client(**docker_environment)

        response = self._docker_client.pull(INFLUXDB_DOCKERHUB_REPOSITORY, INFLUXDB_VERSION)
        logging.debug(response)

        # FIXME
        http_api_port = _get_port()

        self._container = self._docker_client.create_container(
            image=INFLUXDB_DOCKERHUB_REPOSITORY,
            ports=[http_api_port],
            host_config=self._docker_client.create_host_config(
                port_bindings={
                    8086: http_api_port
                }
            )
        )

        # atexit.register(kill_container)
        self._docker_client.start(self._container)
        logging.info("Waiting for InfluxDB server to setup")
        for line in self._docker_client.logs(self._container, stream=True):
            logging.debug(line)
            if "Listening for signals" in str(line):
                break

        url = urlparse(self._docker_client.base_url)
        host = url.hostname if url.scheme in ["http", "https"] else "localhost"
        host = url.hostname

        self._influxdb_client = InfluxDBClient(host, http_api_port, "root", "root")
        self._influxdb_client.create_database("my_db")

        connection_config = InfluxDBConnectionConfig(host, http_api_port, "root", "root", "my_db")
        print(connection_config)

        self._log_recorder = InfluxDBLogRecorder(connection_config)

    def tearDown(self):
        self._docker_client.kill(self._container)

    def test_log(self):
        log = Log("measuring", 123, {"a": 1}, datetime(2016, 3, 2))
        self._log_recorder.log(log)

        points = self._influxdb_client.get_list_series("my_db")
        print(points)
        a = InfluxDBLogJSONDecoder().decode_dict(points)
        print(a)
        print(log)
        self.assertEqual(a, [log])

    # TODO: test static tags



if __name__ == "__main__":
    unittest.main()
