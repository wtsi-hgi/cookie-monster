from sqlalchemy import Column, Integer, DateTime, Interval
from sqlalchemy.ext.declarative import declarative_base


SQLAlchemyModel = declarative_base()


class SQLAlchemyRetrievalLog(SQLAlchemyModel):
    """
    Model of an entry in the retrieve log database, for use with SQLAlchemy.
    """
    __tablename__ = "retrieve_log"

    number_of_file_updates = Column(Integer)
    time_taken_to_complete_query = Column(Interval)
    latest_retrieved_timestamp = Column(DateTime)
    id = Column(Integer, autoincrement=True, primary_key=True)
