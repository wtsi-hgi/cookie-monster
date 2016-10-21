"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from os.path import dirname, join, normpath, realpath
from typing import Dict
from testwithirods.helpers import SetupHelper


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

        setup_helper.run_icommand(["iadmin", "asq", "%s" % query, alias])
