from os.path import dirname, join, normpath
from os.path import realpath
from typing import Dict

from testwithbaton import SetupHelper


def install_queries(required_specific_queries: Dict[str, str], setup_helper: SetupHelper):
    """
    Installs the given specific queries required to in iRODS using the given iRODS setup helper.
    :param required_specific_queries: dictionary of specific queries, where the key is the alias and the value is the
    SQL query
    :param setup_helper: iRODS setup helper
    """
    for alias, query_location_relative_to_root in required_specific_queries.items():
        query_location = normpath(join(dirname(realpath(__file__)), "..", "..", "..", "..", "..",
                                       query_location_relative_to_root))
        with open(query_location) as file:
            query = file.read().replace('\n', ' ')

        setup_helper.run_icommand(["iadmin", "asq", "\"%s\"" % query, alias])
