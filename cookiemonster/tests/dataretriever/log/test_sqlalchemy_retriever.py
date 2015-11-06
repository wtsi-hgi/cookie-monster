import tempfile
import unittest

import sqlite3
from datetime import timedelta, date

from sqlalchemy import MetaData, create_engine, Table

from cookiemonster.common.sqlalchemy_database_connector import SQLAlchemyDatabaseConnector
from cookiemonster.dataretriever._models import RetrievalLog
from cookiemonster.dataretriever.log.slqalchemy_retriever import SQLAlchemyRetrievalLogMapper
from cookiemonster.dataretriever.log.sqlalchemy_models import SQLAlchemyRetrievalLog, SQLAlchemyModel


class TestSQLAlchemyRetrievalLogMapper(unittest.TestCase):
    def setUp(self):
        pass

    def test_something(self):
        database_connector = self._create_connector()
        mapper = SQLAlchemyRetrievalLogMapper(database_connector)
        mapper.add(RetrievalLog(1, "abc", "abc"))
        print(mapper.get_most_recent())
        self.assertEqual(True, False)

    @staticmethod
    def _create_connector() -> SQLAlchemyDatabaseConnector:
        file_handle, database_location = tempfile.mkstemp()
        engine = create_engine("sqlite:///%s" % database_location)
        metadata = MetaData()

        SQLAlchemyModel.metadata.create_all(bind=engine)


        return SQLAlchemyDatabaseConnector("sqlite:///%s" % database_location)


if __name__ == '__main__':
    unittest.main()
