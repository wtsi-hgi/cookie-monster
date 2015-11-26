from datetime import timedelta, datetime

from sqlalchemy import create_engine

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.sqlalchemy import SQLAlchemyDatabaseConnector
from cookiemonster.retriever.irods.baton_retriever import BatonFileUpdateRetriever
from cookiemonster.retriever.irods.irods_config import IrodsConfig
from cookiemonster.retriever.log.sqlalchemy_mappers import SQLAlchemyRetrievalLogMapper
from cookiemonster.retriever.log._sqlalchemy_models import SQLAlchemyModel
from cookiemonster.retriever.manager import PeriodicRetrievalManager
from cookiemonster.cookiejar import CookieJar


def main():
    # TODO: These values need to be loaded from config file/passed in through CLI
    retrieval_log_database_location = ""
    retrieval_period = timedelta()
    file_updates_since = datetime.now()
    workflow_database_location = "foo.sqlite"
    metadata_database_host = "http://localhost:5984"
    metadata_database_name = "foo"
    failure_lead_time = timedelta(days=5)

    # TODO: Setup other things
    cookie_jar = CookieJar(
        workflow_database_location,
        metadata_database_host,
        metadata_database_name,
        failure_lead_time
    )

    # Coordinates setup of data retrieval
    retrieval_manager = create_retrieval_manager(retrieval_period, retrieval_log_database_location)
    retrieval_manager.add_listener(on_file_updates_retrieved)
    retrieval_manager.add_listener(cookie_jar)
    retrieval_manager.start(file_updates_since)


def on_file_updates_retrieved(file_updates: FileUpdateCollection):
    """
    Method that will be called when file updates are retrieved.
    :param file_updates: the file updates that have been retrieved
    """
    raise NotImplementedError()


def setup_retrieval_log_database(database_location : str):
    """
    Uses the database in the given location for storing retrieval logs. If the database does not exist, a new SQLite
    database will be created. Any type of database supported by SQLAlchemy may be used.
    :param database_location: the location of teh retrieval log database
    """
    engine = create_engine(database_location)
    # TODO: Need to process_any_jobs if the database is already there and what create_all does in this situation
    SQLAlchemyModel.metadata.create_all(bind=engine)


def create_retrieval_manager(retrieval_period: timedelta, retrieval_log_database_location: str) -> PeriodicRetrievalManager:
    """
    Factory function for creating a file update retrieval manager.
    :param retrieval_period: the period between file update retrieves
    :param retrieval_log_database_location: the location of the database in which retrieval logs are to be stored
    :return: the retrieval manager
    """
    irods_config = IrodsConfig()
    file_update_retriever = BatonFileUpdateRetriever(irods_config)

    database_connector = SQLAlchemyDatabaseConnector(retrieval_log_database_location)
    retrieval_log_mapper = SQLAlchemyRetrievalLogMapper(database_connector)

    retrieval_manager = PeriodicRetrievalManager(retrieval_period, file_update_retriever, retrieval_log_mapper)
    return retrieval_manager
