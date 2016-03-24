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
from hgicommon.models import Model

from cookiemonster.logging.models import Log


class InfluxDBConnectionConfig(Model):
    """
    Connection configuration to an InfluxDB database.
    """
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database


class InfluxDBLog(Log):
    """
    Log used by InfluxDB.
    """
    @staticmethod
    def value_of(log: Log):
        """
        Static factory method to build an intance of this type from its superclass.
        :param log: log to build instance of this type from
        :return: instance of this type, based of the log given
        """
        if isinstance(log, InfluxDBLog):
            return log
        return InfluxDBLog(log.measured, log.values, log.metadata, log.timestamp)
