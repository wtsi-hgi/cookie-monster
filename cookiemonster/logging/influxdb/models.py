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
        return InfluxDBLog(log.measuring, log.value, log.metadata, log.timestamp)
