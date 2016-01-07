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
Set up and launch the API service as a separate process

* `inject` Link a Cookie Monster dependency into the service

* `listen` Start the service as a separate process, listening on the
  specified port

* `stop` Forcibly stop the running service

Routes
------
The following routes have been specified:

    /queue
      GET   The current processing queue length

    /queue/reprocess
      POST  Mark the file in the request's `.path` for reprocessing

FIXME While we don't have any hypermedia, sources and sinks goes against
      proper RESTful design. However, this is largely from the lack of
      useful exposed methods that could use parametrised routes
      productively...

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

from enum import Enum
from multiprocessing import Process

from cookiemonster.elmo._framework import API, HTTPMethod, HTTPSource

# Data source handlers and models
from cookiemonster.elmo._cookiejar_handlers import CookieJarHandlers, QueueLength, CookiePath


class APIDependency(Enum):
    ''' Dependency injection enumeration and mapping '''
    CookieJar = CookieJarHandlers


class HTTP_API(object):
    ''' HTTP API service '''
    def __init__(self):
        self._api = API('elmo')
        self._dependencies = {}
        self._service = None

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
        start it as a separate process

        @param  port  The port to listen for HTTP requests
        '''
        api = self._api
        dep = self._dependencies

        # Check all dependencies are satisfied
        for d in APIDependency:
            if d not in dep:
                raise KeyError('Dependencies not fully satisfied; missing {}'.format(d.name))

        # Build service
        api.create_route('/queue', QueueLength) \
           .set_method_handler(HTTPMethod.GET, dep[APIDependency.CookieJar].GET_queue_length)

        api.create_route('/queue/reprocess', CookiePath) \
           .set_method_handler(HTTPMethod.POST, dep[APIDependency.CookieJar].POST_mark_for_processing)

        # Start service
        self._service = Process(target=api.listen, args=(port,))
        self._service.start()

    def stop(self):
        '''
        Stop the running service
        '''
        if self._service:
            self._service.terminate()
            self._service.join()
