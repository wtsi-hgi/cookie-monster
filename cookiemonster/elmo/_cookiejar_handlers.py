"""
Cookie Jar API Handlers
=======================
Exportable classes: `CookieJarHandlers`

CookieJarHandlers
-----------------
Method handlers for the Cookie Jar:

* `GET_queue_length` GET handler for the Cookie Jar "to process" queue
  length; returns a dictionary with a `queue_length` member

* `POST_mark_for_processing` POST handler for marking cookies for
  (re)processing; expects either a plain string or a dictionary with a
  `identifier` string member in the request data, returns a dictionary
  with a `identifier` member

* `GET_cookie` GET handler for fetching cookie data by its identifier

* `DELETE_cookie` DELETE handler for removing a cookie by its identifier

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

from typing import Any

from werkzeug.exceptions import NotFound

from cookiemonster.common.helpers import EnrichmentJSONEncoder
from cookiemonster.cookiejar import BiscuitTin
from cookiemonster.elmo._handler_injection import DependencyInjectionHandler


class CookieJarHandlers(DependencyInjectionHandler):
    """ Handler functions for CookieJar """
    def GET_queue_length(self, **kwargs):
        cookiejar = self._dependency
        return {'queue_length': cookiejar.queue_length()}

    def POST_mark_for_processing(self, data:Any, **kwargs):
        cookiejar = self._dependency

        if isinstance(data, str):
            cookie = {'identifier': data}
        elif isinstance(data, dict) and 'identifier' in data:
            cookie = {'identifier': data['identifier']}
        else:
            raise ValueError()

        cookiejar.mark_for_processing(cookie['identifier'])
        return cookie

    def GET_cookie(self, **kwargs):
        cookiejar = self._dependency

        # Get the identifier from the query string first,
        # then look at the URL parameter
        identifier = kwargs['_query'].get('identifier')
        if not identifier:
            identifier = kwargs['identifier']

        cookie = cookiejar.fetch_cookie(identifier)
        if not cookie:
            raise NotFound

        # TODO: This defines a JSON representation of a Cookie that could be encapsulated in a JSONEncoder
        enrichments = EnrichmentJSONEncoder().default(list(cookie.enrichments))
        return {'identifier':cookie.identifier, 'enrichments':enrichments}

    def DELETE_cookie(self, **kwargs):
        cookiejar = self._dependency

        # Get the identifier from the query string first,
        # then look at the URL parameter
        identifier = kwargs['_query'].get('identifier')
        if not identifier:
            identifier = kwargs['identifier']

        cookie = cookiejar.fetch_cookie(identifier)
        if not cookie:
            raise NotFound

        cookiejar.delete_cookie(identifier)
        return {'deleted':identifier}
