from sqlalchemy import Column, Integer, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base


SQLAlchemyModel = declarative_base()


class SQLAlchemyRetrievalLog(SQLAlchemyModel):
    """
    Model of an entry in the retrieve log database, for use with SQLAlchemy.
    """
    __tablename__ = "RetrieveLog"
    started_at = Column(DateTime)
    seconds_taken_to_complete_query = Column(Float)
    number_of_file_updates = Column(Integer)
    latest_retrieved_timestamp = Column(DateTime)
    id = Column(Integer, autoincrement=True, primary_key=True)
