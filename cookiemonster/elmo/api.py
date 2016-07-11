"""
Cookie Monster HTTP API
=======================
The external HTTP-based API for interacting with Cookie Monster.

Exportable classes: `HTTP_API`, APIDependency (enum)

APIDependency
-------------
Enumeration of Cookie Monster dependencies, which is (ab)used to map to
handler objects, which are in turn injected with their appropriate
dependency.

HTTP_API
--------
Set up and launch the API service on a separate thread

* `inject` Link a Cookie Monster dependency into the service

* `listen` Start the service on a separate thread, listening on the
  specified port

* `stop` Forcibly stop the running service

Routes
------
The following routes have been specified:

    /queue
      GET     The current processing queue length

    /queue/reprocess
      POST    Mark the file in the request's `.path` for reprocessing

    /cookiejar?identifier=<identifier>
    /cookiejar/<identifier>
      GET     Fetch cookie by identifier
      DELETE  Delete cookie by identifier

      n.b. The query string version of this route is to accommodate
      identifiers that start with a leading slash

    /cookiejar/unjam - NOT FOR GENERAL USE!!
      POST    Forcibly unlock CouchDB jammed batching threads

    /debug/threads
      GET     Dump thread debugging data

NOTE This is not True RESTfulness, as we have no hypermedia; but this is
from lack of a sufficiently non-trivial graph to model

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

from enum import Enum
from threading import Thread

from cookiemonster.elmo._framework import API, HTTPMethod

# Data source handlers
from cookiemonster.elmo._cookiejar_handlers import CookieJarHandlers
from cookiemonster.elmo._system_handlers import SystemHandlers


class APIDependency(Enum):
    """ Dependency injection enumeration and mapping """
    CookieJar = CookieJarHandlers
    System = SystemHandlers


class HTTP_API(object):
    """ HTTP API service """
    def __init__(self):
        self._api = API('elmo')
        self._handlers = {}
        self._service = None

    def inject(self, name:APIDependency, dependency:object):
        """
        Inject a Cookie Monster dependency into the service
        
        @param  name        Dependency name
        @param  dependency  Cookie Monster dependency
        """
        handler = name.value()
        handler.inject_dependency(dependency)
        self._handlers[name] = handler

    def listen(self, port:int=5000):
        """
        Check all dependencies are satisfied, define the service and
        start it on a separate thread

        @param  port  The port to listen for HTTP requests
        """
        api = self._api
        dep = self._handlers

        # Check all dependencies are satisfied
        for d in APIDependency:
            if d not in dep:
                raise KeyError('Dependencies not fully satisfied; missing {}'.format(d.name))

        # Build service
        api.create_route('/queue') \
           .set_method_handler(HTTPMethod.GET, dep[APIDependency.CookieJar].GET_queue_length)

        api.create_route('/queue/reprocess') \
           .set_method_handler(HTTPMethod.POST, dep[APIDependency.CookieJar].POST_mark_for_processing)

        api.create_route('/cookiejar') \
            .set_method_handler(HTTPMethod.GET, dep[APIDependency.CookieJar].GET_cookie) \
            .set_method_handler(HTTPMethod.DELETE, dep[APIDependency.CookieJar].DELETE_cookie)

        api.create_route('/cookiejar/<path:identifier>') \
            .set_method_handler(HTTPMethod.GET, dep[APIDependency.CookieJar].GET_cookie) \
            .set_method_handler(HTTPMethod.DELETE, dep[APIDependency.CookieJar].DELETE_cookie)

        api.create_route('/cookiejar/unjam') \
            .set_method_handler(HTTPMethod.POST, dep[APIDependency.CookieJar].POST_unjam)

        api.create_route('/debug/threads') \
            .set_method_handler(HTTPMethod.GET, dep[APIDependency.System].GET_thread_dump)

        # Start service
        self._service = Thread(target=api.listen, args=(port,), daemon=True)
        self._service.start()

    def stop(self):
        """ Stop the running service """
        if self._service.is_alive():
            self._api.stop()
            self._service.join()
