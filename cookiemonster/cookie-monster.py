import logging
import tempfile
from datetime import timedelta, datetime, date
from time import sleep

from hgicommon.collections import Metadata
from mock import MagicMock
from sqlalchemy import create_engine

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.enums import EnrichmentSource
from cookiemonster.common.models import FileUpdate, Notification, Enrichment
from cookiemonster.common.sqlalchemy import SQLAlchemyDatabaseConnector
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.notifier.printing_notifier import PrintingNotifier
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._models import Rule, RuleAction, EnrichmentLoader
from cookiemonster.processor._rules import RulesManager
from cookiemonster.processor.processing import ProcessorManager
from cookiemonster.processor.basic_processing import BasicProcessorManager
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

    manager_db_host = "http://localhost:5984"
    manager_db_prefix = "cookiemonster"

    # Setup database for retrieval log
    setup_retrieval_log_database(retrieval_log_database_location)

    # Setup data retrieval manager
    retrieval_manager = create_retrieval_manager(retrieval_period, retrieval_log_database_location)

    # Setup data manager (loads more data about a file)
    data_loader_manager = EnrichmentManager()

    # Setup cookie jar
    # cookie_jar = BiscuitTin(manager_db_host, manager_db_prefix)
    cookie_jar = InMemoryCookieJar()    # type: CookieJar

    # Setup rules manager
    rules_manager = RulesManager()

    # Setup notifier
    notifier = PrintingNotifier()   # type: Notifier

    # Setup the data processor manager
    processor_manager = BasicProcessorManager(
        number_of_processors, cookie_jar, rules_manager, data_loader_manager, notifier)    # type: ProcessorManager

    # Connect the cookie jar to the retrieval manager
    def put_file_update_in_cookie_jar(file_updates: FileUpdateCollection):
        for file_update in file_updates:
            enrichment = Enrichment(EnrichmentSource.IRODS_UPDATE, datetime.now(), file_update.metadata)
            cookie_jar.enrich_cookie(file_update.file_id, enrichment)
    retrieval_manager.add_listener(put_file_update_in_cookie_jar)

    # Connect the data processor manager to the cookie jar
    def prompt_processor_manager_to_process_new_jobs(*args):
        processor_manager.process_any_cookie_jobs()
    cookie_jar.add_listener(prompt_processor_manager_to_process_new_jobs)


    # Let's see if the setup works!
    file_update_1 = FileUpdate("file_id_1", hash("hash"), datetime(year=2000, month=9, day=10), Metadata())
    query_result_1 = QueryResult(FileUpdateCollection([file_update_1]), timedelta(0))

    file_update_2 = FileUpdate("file_id_2", hash("hash"), datetime(year=2001, month=8, day=7), Metadata())
    query_result_2 = QueryResult(FileUpdateCollection([file_update_2]), timedelta(0))

    blank_query_results = [QueryResult(FileUpdateCollection(), timedelta(0)) for _ in range(1000)]

    retrieval_manager._file_update_retriever.query_for_all_file_updates_since = MagicMock(
        side_effect=[query_result_1, query_result_2] + blank_query_results)

    rule_1 = Rule(
        lambda cookie: cookie.path == "file_id_1",
        lambda cookie: RuleAction([Notification("External process interested in file_id_1")], cookie))
    rules_manager.add_rule(rule_1)
    rule_2 = Rule(
        lambda cookie: cookie.path == "file_id_2",
        lambda cookie: RuleAction([Notification("External process interested in file_id_2")], cookie))
    rules_manager.add_rule(rule_2)

    # Start the data retrieval manger
    retrieval_manager.start(file_updates_since)
    logging.debug("Started retrieval manager")


def setup_retrieval_log_database(database_location : str):
    """
    Uses the database in the given location for storing retrieval logs. If the database does not exist, a new SQLite
    database will be created. Any type of database supported by SQLAlchemy may be used.
    :param database_location: the location of teh retrieval log database
    """
    engine = create_engine(database_location)
    # TODO: Need to process_any_cookie_jobs if the database is already there and what create_all does in this situation
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
