'''
Cookie Monster HTTP API
=======================
The external HTTP-based API for interacting with Cookie Monster.

Exportable classes: `HTTP_API`, APIDependency

APIDependency
-------------
Enumeration of Cookie Monster dependencies

HTTP_API
--------
Set up and launch the API service on a separate thread

* `inject` Link a Cookie Monster dependency into the service

* `listen` Start the service on a separate thread, listening on the
  specified port

Routes
------
The following routes have been specified:

    /cookie-jar/queue-length
      GET  The current processing queue length

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

from enum import Enum

from cookiemonster.elmo._framework import API, HTTPMethod, HTTPSource

# Data source handlers and models
from cookiemonster.elmo._cookiejar_handlers import CookieJarHandlers, QueueLength


class APIDependency(Enum):
    ''' Dependency injection enumeration and mapping '''
    CookieJar = CookieJarHandlers


class HTTP_API(object):
    ''' HTTP API service '''
    def __init__(self):
        self._api = API('elmo')
        self._dependencies = {}

    def inject(self, name:APIDependency, dependency:object):
        '''
        Inject a Cookie Monster dependency into the service
        
        @param  name        Dependency name
        @param  dependency  Cookie Monster dependency
        '''
        self._dependencies[name] = name.value(dependency)

    def listen(self, port:int=5000):
        '''
        Check all dependencies are satisfied, define the service and
        start it on a separate thread

        @param  port  The port to listen for HTTP requests
        '''
        api = self._api
        dep = self._dependencies

        # Check all dependencies are satisfied
        for d in APIDependency:
            if d not in dep:
                raise KeyError('Dependencies not fully satisfied; missing {}'.format(d.name))

        # Build service
        api.create_route('/cookie-jar/queue-length', QueueLength) \
           .set_method_handler(HTTPMethod.GET, dep[APIDependency.CookieJar].get_queue_length)

        # TODO Threading
        api.listen(port)
