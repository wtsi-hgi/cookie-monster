from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


class SQLAlchemyDatabaseConnector:
    """
    Database connector for use with SQLAlchemy.
    """
    def __init__(self, database_location : str):
        """
        Constructor.
        :param database_location: the url of the database that connections can be made to.
        """
        self._engine = None
        self._database_location = database_location

    def create_session(self) -> Session:
        """
        Creates a SQLAlchemy session, which is used to interact with the database.
        :return: connected database session
        """
        if not self._engine:
            self._engine = create_engine(self._database_location)

        Session = sessionmaker(bind=self._engine)
        session = Session()
        return session
