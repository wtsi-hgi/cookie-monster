import logging
import tempfile
from datetime import timedelta, datetime

from hgicommon.collections import Metadata
from hgicommon.data_source import DataSource
from hgicommon.data_source import ListDataSource
from mock import MagicMock
from sqlalchemy import create_engine

from cookiemonster import Notification, Rule, RuleAction
from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.enums import EnrichmentSource
from cookiemonster.common.models import Enrichment
from cookiemonster.common.models import Update
from cookiemonster.common.sqlalchemy import SQLAlchemyDatabaseConnector
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.notifier.printing_notifier import PrintingNotifier
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor.basic_processing import BasicProcessorManager
from cookiemonster.processor.processing import ProcessorManager
from cookiemonster.retriever.log._sqlalchemy_models import SQLAlchemyModel
from cookiemonster.retriever.log.sqlalchemy_mapper import SQLAlchemyRetrievalLogMapper
from cookiemonster.retriever.manager import PeriodicRetrievalManager
from cookiemonster.elmo import HTTP_API, APIDependency
from cookiemonster.tests.retriever._stubs import StubUpdateMapper


def main():
    # TODO: These values need to be loaded from config file/passed in through CLI
    retrieval_log_database_location = "sqlite:///%s" % tempfile.mkstemp()[1]
    retrieval_period = timedelta(seconds=5)
    updates_since = datetime.min

    number_of_processors = 5

    manager_db_host = "http://localhost:5984"
    manager_db_name = "cookiemonster"

    http_api_port = 5000

    # Setup database for retrieval log
    setup_retrieval_log_database(retrieval_log_database_location)

    # Setup data retrieval manager
    retrieval_manager = create_retrieval_manager(retrieval_period, retrieval_log_database_location)

    # Setup enrichment manager
    enrichment_manager = EnrichmentManager()

    # Setup cookie jar
    # cookie_jar = BiscuitTin(manager_db_host, manager_db_name)
    cookie_jar = InMemoryCookieJar()    # type: CookieJar

    # Setup rules source
    rules = []
    rules_source = ListDataSource(rules) # type: DataSource[Rule]

    # Setup notifier
    notifier = PrintingNotifier()   # type: Notifier

    # Setup the data processor manager
    processor_manager = BasicProcessorManager(
        number_of_processors, cookie_jar, rules_source, enrichment_manager, notifier)    # type: ProcessorManager

    # Setup the HTTP API
    api = HTTP_API()
    api.inject(APIDependency.CookieJar, cookie_jar)
    api.listen(http_api_port)

    # Connect the cookie jar to the retrieval manager
    def put_update_in_cookie_jar(update_collection: UpdateCollection):
        for update in update_collection:
            enrichment = Enrichment(EnrichmentSource.IRODS_UPDATE, datetime.now(), update.metadata)
            cookie_jar.enrich_cookie(update.target, enrichment)
    retrieval_manager.add_listener(put_update_in_cookie_jar)

    # Connect the data processor manager to the cookie jar
    def prompt_processor_manager_to_process_new_jobs(*args):
        processor_manager.process_any_cookies()
    cookie_jar.add_listener(prompt_processor_manager_to_process_new_jobs)


    # Let's see if the setup works!
    updates_1 = UpdateCollection([
        Update("file_id_1", hash("hash"), datetime(year=2000, month=9, day=10), Metadata())
    ])
    updates_2 = UpdateCollection([
        Update("file_id_2", hash("hash"), datetime(year=2001, month=8, day=7), Metadata())
    ])
    no_updates = [UpdateCollection() for _ in range(1000)]

    retrieval_manager.update_mapper.get_all_since = MagicMock(side_effect=[updates_1, updates_2] + no_updates)

    rule_1 = Rule(
        lambda cookie: cookie.path == "file_id_1",
        lambda cookie: RuleAction([Notification("External process interested in file_id_1", cookie.path)], True))
    rules.append(rule_1)
    rule_2 = Rule(
        lambda cookie: cookie.path == "file_id_2",
        lambda cookie: RuleAction([Notification("External process interested in file_id_2", cookie.path)], True))
    rules.append(rule_2)

    # Start the data retrieval manger
    retrieval_manager.start(updates_since)
    logging.debug("Started retrieval manager")


def setup_retrieval_log_database(database_location : str):
    """
    Uses the database in the given location for storing retrieval logs. If the database does not exist, a new SQLite
    database will be created. Any type of database supported by SQLAlchemy may be used.
    :param database_location: the location of teh retrieval log database
    """
    engine = create_engine(database_location)
    # TODO: Need to process_any_cookies if the database is already there and what create_all does in this situation
    SQLAlchemyModel.metadata.create_all(bind=engine)


def create_retrieval_manager(retrieval_period: timedelta, retrieval_log_database_location: str) \
        -> PeriodicRetrievalManager:
    """
    Factory function for creating a file update retrieval manager.
    :param retrieval_period: the period between file update retrieves
    :param retrieval_log_database_location: the location of the database in which retrieval logs are to be stored
    :return: the retrieval manager
    """
    update_mapper = StubUpdateMapper()

    database_connector = SQLAlchemyDatabaseConnector(retrieval_log_database_location)
    retrieval_log_mapper = SQLAlchemyRetrievalLogMapper(database_connector)

    retrieval_manager = PeriodicRetrievalManager(retrieval_period, update_mapper, retrieval_log_mapper)
    return retrieval_manager


if __name__ == "__main__":
    logging.root.setLevel("DEBUG")
    main()
