"""
Logging Dependency Injection
============================
Provides runtime logging for CouchDB calls performed by SofterCouchDB
(in cookiemonster.cookiejar.couchdb.softer) via dependency injection

Exportable functions: `inject_logging`
Exportable classes: `LoggingSofterCouchDB`

inject_logging
--------------
`inject_logging` will apply runtime logging to an instantiated
SofterCouchDB's methods, using the specified logger.

LoggingSofterCouchDB
--------------------
`LoggingSofterCouchDB` instantiates a `SofterCouchDB` with logging
applied to its methods.

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
import time
from typing import Any, Callable

from cookiemonster.cookiejar.couchdb.softer import SofterCouchDB
from cookiemonster.logging.logger import Logger


def _log_runtime(fn:Callable[..., Any], logger:Logger) -> Callable[..., Any]:
    """
    Decorates the function with runtime logging

    @param   fn      The function to decorate
    @param   logger  The logger to write to
    @return  Decorated function
    """
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        output = fn(*args, **kwargs)
        runtime = time.monotonic() - start

        logger.record('couchdb_runtime', runtime, {'function': fn.__name__})
        return output

    return wrapper


def inject_logging(db:SofterCouchDB, logger:Logger):
    """
    Inject runtime logging into a SofterCouchDB instance, targeting all
    callable, "public" attributes

    @param   db      The SofterCouchDB instance to inject into
    @param   logger  The logger to write to
    """
    for method in db._db_methods:
        setattr(db, method, _log_runtime(getattr(db, method), logger))


class LoggingSofterCouchDB(SofterCouchDB):
    def __init__(self, logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        inject_logging(self, logger)
