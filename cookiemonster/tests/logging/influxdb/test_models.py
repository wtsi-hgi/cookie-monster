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
import unittest
from datetime import datetime

from cookiemonster.logging.influxdb.models import InfluxDBLog
from cookiemonster.logging.models import Log

_MEASUREMENT_NAME = "measurement_name"
_MEASUREMENT_VALUE = 123
_MEASUREMENT_METADATA = {"a": 1, "b": 2}
_MEASUREMENT_TIMESTAMP = datetime(2000, 1, 1)


class TestInfluxDBLog(unittest.TestCase):
    """
    Tests for `InfluxDBLog`.
    """
    def setUp(self):
        self._log = Log(
            _MEASUREMENT_NAME, _MEASUREMENT_VALUE, _MEASUREMENT_METADATA, _MEASUREMENT_TIMESTAMP)
        self._influx_db_log = InfluxDBLog(
            _MEASUREMENT_NAME, _MEASUREMENT_VALUE, _MEASUREMENT_METADATA, _MEASUREMENT_TIMESTAMP)

    def test_value_of_with_log(self):
        self.assertEqual(InfluxDBLog.value_of(self._log), self._influx_db_log)

    def test_value_of_with_influx_db_log(self):
        self.assertEqual(InfluxDBLog.value_of(self._influx_db_log), self._influx_db_log)


if __name__ == "__main__":
    unittest.main()
