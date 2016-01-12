from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.log.sqlalchemy_models import SQLAlchemyRetrievalLog


def convert_to_retrieval_log(sqlalchemy_retrieval_log: SQLAlchemyRetrievalLog) -> RetrievalLog:
    """
    Converts the given SQLAlchemy model to an equivalent `RetrieveLog` POPO model.
    :param sqlalchemy_retrieval_log: SQLAlchemy model of a retrieval log
    :return: the equivalent POPO model of a retrieval log
    """
    return RetrievalLog(
        sqlalchemy_retrieval_log.latest_retrieved_timestamp,
        sqlalchemy_retrieval_log.number_of_file_updates,
        sqlalchemy_retrieval_log.time_taken_to_complete_query)


def convert_to_sqlalchemy_retrieval_log(retrieval_log: RetrievalLog) -> SQLAlchemyRetrievalLog:
    """
    Creates an instance equivalent to the given `RetrievalLog` POPO model.
    :param retrieval_log: POPO model of a retrieval log
    :return: the equivalent SQLAlchemy model of a retrieval log
    """
    sqlalchemy_retrieval_log = SQLAlchemyRetrievalLog()
    sqlalchemy_retrieval_log.number_of_file_updates = retrieval_log.number_of_updates
    sqlalchemy_retrieval_log.time_taken_to_complete_query = retrieval_log.time_taken_to_complete_query
    sqlalchemy_retrieval_log.latest_retrieved_timestamp = retrieval_log.retrieved_updates_since
    return sqlalchemy_retrieval_log
