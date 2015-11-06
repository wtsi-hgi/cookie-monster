from sqlalchemy import Column, Integer, DateTime, Interval
from sqlalchemy.ext.declarative import declarative_base

from cookiemonster.dataretriever._models import RetrievalLog

SQLAlchemyModel = declarative_base()


class SQLAlchemyRetrievalLog(SQLAlchemyModel):
    """
    Model of an entry in the retrieve log database, for use with SQLAlchemy.
    """
    __tablename__ = "retrieve_log"
    number_of_file_updates = Column(Integer)
    time_taken_to_complete_query = Column(Interval)
    latest_retrieved_timestamp = Column(DateTime, primary_key=True)   # TODO: Is it correct to use this as the primary key?

    def to_retrieval_log(self) -> RetrievalLog:
        """
        Converts the SQLAlchemy model to an equivalent `RetrieveLog` POPO model.
        :return: the equivalent `RetrieveLog` model
        """
        return RetrievalLog(
            self.latest_retrieved_timestamp,
            self.number_of_file_updates,
            self.time_taken_to_complete_query)

    # TODO: Fix type hinting for below
    @staticmethod
    def value_of(retrieval_log: RetrievalLog):
        """
        Creates an instance equivalent to the given `RetrievalLog` POPO model.
        :return: the equivalent `SQLAlchemyRetrievalLog` model
        """
        sqlalchemy_retrieval_log = SQLAlchemyRetrievalLog()
        sqlalchemy_retrieval_log.number_of_file_updates = retrieval_log.number_of_file_updates
        sqlalchemy_retrieval_log.time_taken_to_complete_query = retrieval_log.time_taken_to_complete_query
        sqlalchemy_retrieval_log.latest_retrieved_timestamp = retrieval_log.latest_retrieved_timestamp
        return sqlalchemy_retrieval_log


