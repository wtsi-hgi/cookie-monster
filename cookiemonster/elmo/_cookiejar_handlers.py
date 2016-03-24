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

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""

import json
from typing import Any

from werkzeug.exceptions import NotFound

from cookiemonster.common.helpers import EnrichmentJSONEncoder
from cookiemonster.elmo._handler_injection import DependencyInjectionHandler


class CookieJarHandlers(DependencyInjectionHandler):
    """ Handler functions for CookieJar """
    def GET_queue_length(self):
        cookiejar = self._dependency
        return {'queue_length': cookiejar.queue_length()}

    def POST_mark_for_processing(self, data:Any):
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

        # Try raw identifier first; if that fails, try absolute path
        # (This is because prepending the slash in the URL won't work)
        identifier = kwargs['cookie']
        cookie = cookiejar.fetch_cookie(identifier) \
              or cookiejar.fetch_cookie('/{}'.format(identifier))

        if not cookie:
            raise NotFound

        # FIXME? Back-and-forward JSON decoding :P
        enrichments = json.loads(json.dumps(cookie.enrichments, cls=EnrichmentJSONEncoder))
        return {'identifier':cookie.identifier, 'enrichments':enrichments}

    def DELETE_cookie(self, **kwargs):
        cookiejar = self._dependency

        identifier = kwargs['cookie']
        cookie = cookiejar.fetch_cookie(identifier)

        # Try raw identifier first; if that fails, try absolute path
        # (This is because prepending the slash in the URL won't work)
        if not cookie:
            identifier = '/{}'.format(identifier)
            cookie = cookiejar.fetch_cookie(identifier)

        cookie = cookiejar.fetch_cookie(identifier)
        if not cookie:
            raise NotFound

        cookiejar.delete_cookie(identifier)
        return {'deleted':identifier}
