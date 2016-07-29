"""
Logging Dependency Injection
============================
Provides response time logging for CouchDB calls performed by 
SofterCouchDB (in cookiemonster.cookiejar.couchdb.softer) via dependency
injection

Exportable functions: `inject_logging`

inject_logging
--------------
`inject_logging` will apply runtime logging to an instantiated
SofterCouchDB's methods, using the specified logger.

Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

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
from typing import Dict, Optional

from cookiemonster.cookiejar.couchdb.softer import SofterCouchDB
from cookiemonster.logging.logger import Logger
from cookiemonster.logging.injector import LoggingContext, RuntimeLogging, LoggingMapper


class _CouchDBResponseTimeLogging(RuntimeLogging):
    """
    Log response times under couchdb with a "function" tag matching the
    called CouchDB function (e.g., save_bulk, etc.)
    """
    def get_measure(self, context:LoggingContext) -> str:
        return 'couchdb'

    def get_metadata(self, context:LoggingContext) -> Optional[Dict]:
        return {'function': context.name}


def inject_logging(db:SofterCouchDB, logger:Logger):
    """
    Inject CouchDB response time logging into a SofterCouchDB instance,
    targeting all callable, "public" attributes. Note that we can only
    inject into instantiated SofterCouchDBs because they are monkey-
    patched themselves; as such, we can't use the mass mapping
    convenience functions of the injector.

    @param   db      The SofterCouchDB instance to inject into
    @param   logger  The logger to write to
    """
    mapping = LoggingMapper(logger)
    mapping.map_logging_to_public_methods(db, _CouchDBResponseTimeLogging)
    mapping.inject_logging(db)
