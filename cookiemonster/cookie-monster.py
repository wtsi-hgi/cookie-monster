import logging
import tempfile
from datetime import timedelta, datetime, date
from time import sleep

from hgicommon.collections import Metadata
from mock import MagicMock
from sqlalchemy import create_engine

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate
from cookiemonster.common.sqlalchemy import SQLAlchemyDatabaseConnector
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.notifier.printing_notifier import PrintingNotifier
from cookiemonster.processor._data_management import DataManager
from cookiemonster.processor._rules_management import RulesManager
from cookiemonster.processor.processor import ProcessorManager
from cookiemonster.processor.simple_processor import SimpleProcessorManager
from cookiemonster.retriever._models import QueryResult
from cookiemonster.retriever.irods.irods_config import IrodsConfig
from cookiemonster.retriever.log._sqlalchemy_models import SQLAlchemyModel
from cookiemonster.retriever.log.sqlalchemy_mappers import SQLAlchemyRetrievalLogMapper
from cookiemonster.retriever.manager import PeriodicRetrievalManager
from cookiemonster.tests.retriever.stubs import StubFileUpdateRetriever


def main():
    # TODO: These values need to be loaded from config file/passed in through CLI
    retrieval_log_database_location = "sqlite:///%s" % tempfile.mkstemp()[1]
    retrieval_period = timedelta(seconds=5)
    file_updates_since = datetime.min

    number_of_processors = 5

    # workflow_database_location = "foo.sqlite"
    # metadata_database_host = "http://localhost:5984"
    # metadata_database_name = "foo"
    # failure_lead_time = timedelta(days=5)

    # Setup database for retrieval log
    setup_retrieval_log_database(retrieval_log_database_location)

    # Setup data retrieval manager
    retrieval_manager = create_retrieval_manager(retrieval_period, retrieval_log_database_location)

    # Setup data manager (loads more data about a file)
    data_manager = DataManager()

    # Setup cookie jar
    cookie_jar = InMemoryCookieJar()    # type: CookieJar

    # Setup rules manager
    rules_manager = RulesManager()

    # Setup notifier
    notifier = PrintingNotifier()   # type: Notifier

    # Setup the data processor manager
    processor_manager = SimpleProcessorManager(
        number_of_processors, cookie_jar, rules_manager, data_manager, notifier)    # type: ProcessorManager

    # Connect the cookie jar to the retrieval manager
    def put_file_update_in_cookie_jar(file_updates: FileUpdateCollection):
        for file_update in file_updates:
            # FIXME: Metadata is actually a superclass of CookieCrumbs...
            metadata = file_update.metadata
            cookie_jar.enrich_metadata(file_update.file_id, metadata)
    retrieval_manager.add_listener(put_file_update_in_cookie_jar)

    # Connect the data processor manager to the cookie jar
    def prompt_processor_manager_to_process_new_jobs(*args):
        processor_manager.process_any_jobs()
    cookie_jar.add_listener(prompt_processor_manager_to_process_new_jobs)

    file_update = FileUpdate("file_id", hash("hash"), datetime.min, Metadata())
    query_result = QueryResult(FileUpdateCollection([file_update]), timedelta(seconds=42))
    retrieval_manager._file_update_retriever.query_for_all_file_updates_since = MagicMock(side_effect=[query_result])

    # Start the data retrieval manger
    retrieval_manager.start(file_updates_since)
    logging.debug("Started retrieval manager")


    sleep(30)


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
    # file_update_retriever = BatonFileUpdateRetriever(irods_config)
    file_update_retriever = StubFileUpdateRetriever()

    database_connector = SQLAlchemyDatabaseConnector(retrieval_log_database_location)
    retrieval_log_mapper = SQLAlchemyRetrievalLogMapper(database_connector)

    retrieval_manager = PeriodicRetrievalManager(retrieval_period, file_update_retriever, retrieval_log_mapper)
    return retrieval_manager


if __name__ == "__main__":
    logging.root.setLevel("DEBUG")
    main()