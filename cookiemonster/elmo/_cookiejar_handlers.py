'''
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
  `path` string member in the request data, returns a dictionary with a
  `path` member

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

from typing import Any
from cookiemonster.elmo._handler_injection import DependencyInjectionHandler


class CookieJarHandlers(DependencyInjectionHandler):
    ''' Handler functions for CookieJar '''
    def GET_queue_length(self):
        cookiejar = self._dependency
        return {'queue_length': cookiejar.queue_length()}

    def POST_mark_for_processing(self, data:Any):
        cookiejar = self._dependency

        if isinstance(data, str):
            cookie = {'path': data}
        elif isinstance(data, dict) and 'path' in data:
            cookie = {'path': data['path']}
        else:
            raise ValueError()

        cookiejar.mark_for_processing(cookie['path'])
        return cookie
