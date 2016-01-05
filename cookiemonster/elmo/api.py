'''
Cookie Monster HTTP API
=======================
The external HTTP-based API for interacting with Cookie Monster.

Exportable classes: `HTTP_API`

_Dependency
-----------
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

* TODO

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

from enum import Enum

from cookiemonster.elmo._framework import API, HTTPMethod, HTTPSource, HandlerT


''' Dependency injection enumeration '''
_Dependency = Enum('_Dependency', (
    'CookieJar'
)


class HTTP_API(object):
    def __init__(self):
        self._api = API('elmo')
        self._dependencies = {}

    def inject(self, name:_Dependency, dependency:object):
        '''
        Inject a Cookie Monster dependency into the service
        
        @param  name        Dependency name
        @param  dependency  Cookie Monster dependency
        '''
        self._dependencies[name] = dependency

    def listen(self, port:int=5000):
        '''
        Check all dependencies are satisfied, define the service and
        start it on a separate thread

        @param  port The port to listen for HTTP requests
        '''
        for dep in _Dependency:
            if dep not in self._dependencies:
                raise KeyError('Dependencies not fully satisfied; missing {}'.format(dep.value))

        # TODO Build service

        # TODO Threading
        self._api.listen(port)
