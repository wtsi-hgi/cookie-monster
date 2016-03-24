"""
HTTP API Framework
==================
A minimal framework for building an HTTP-based API service, where
parametrised routes can be associated with arbitrary method handlers

Exportable classes: `API`, `Endpoint`, `HTTPMethod` (enum)

API
---
`API` provides the scaffolding to build an HTTP API source, defined by
its routes and method handlers, using the following methods:

* `create_route` Create a route, optionally parametrised, with automatic
  serialisation to/deserialisation from JSON

* `listen` Build the service defined by its routes and method handlers
  and start it, listening on the specified port

* `stop` Stop the running service

Note that the HTTP service will run on localhost, on the specified port.
It is intended to run behind a reverse proxy for external routing,
rather than being given its own host address.

Endpoint
--------
An `Endpoint` encapsulates an API route, with associated method
handlers. The following methods are exposed:

* `set_method_handler` Add/update a method handler function

HTTPMethod
----------
Enumeration of supported HTTP methods:

* GET
* POST
* PUT
* DELETE

OPTIONS and HEAD are provided automatically by the underlying
Flask/Werkzeug implementation. The other methods are not relevant.

Method Chaining
---------------
Note that `API.create_route` and `Endpoint.add_method_handler` both
return an `Endpoint` object, which self-references the route in
question. As such, methods can be chained for convenience:

    api.create_route('/api/<foo>') \
       .set_method_handler(HTTPMethod.GET,  foo_get_handler) \
       .set_method_handler(HTTPMethod.POST, foo_post_handler)

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

import re
import inspect
import collections
import json
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union
from enum import Enum
from uuid import uuid4
from http.client import HTTPConnection

from flask import Flask, request
from werkzeug.exceptions import BadRequest


# Type aliases
_ResponseT = Union[str, Tuple[str, int], Tuple[str, int, dict]]
_HandlerT = Callable[..., Union[Any, Iterable[Any]]]


class HTTPMethod(Enum):
    """ Supported HTTP method enumeration """
    GET    = 'GET'
    POST   = 'POST'
    PUT    = 'PUT'
    DELETE = 'DELETE'


def _has_request_body(method:HTTPMethod) -> bool:
    """
    @param  HTTP method
    @return Whether the above comes with a non-trivial request body
    """
    return method in [HTTPMethod.POST, HTTPMethod.PUT]


class Endpoint(object):
    """ HTTP data source and method handlers """
    def __init__(self, route:str):
        self._route   = route
        self._methods = {}

        # Is the route parametrised?
        self._parametrised = True if re.search('<\w+>', route) else False

    def set_method_handler(self, method:HTTPMethod, handler:_HandlerT) -> 'Endpoint':
        """
        Add/update an HTTP method handler for the data source

        @param  method   The HTTP method
        @param  handler  The method handling function
        @return Itself (i.e., the Endpoint object)
        """
        handler_args = inspect.getfullargspec(handler)

        if self._parametrised and not handler_args.varkw:
            # The handlers for a parametrised route must have kwargs
            raise TypeError('A parametrised route handler must accept keyword arguments')

        if _has_request_body(method) and len(handler_args.args) != 2 if inspect.ismethod(handler) else 1:
            # POST and PUT handlers *must* also have one argument to
            # pass in the deserialised request body
            raise TypeError('A {} handler must have a single argument to accept the request data'.format(method.value))

        if method in self._methods:
            del self._methods[method]

        self._methods[method] = handler
        return self

    def _get_methods(self) -> List[str]:
        """
        @return A list of the defined method strings
        """
        return [method.value for method in self._methods.keys()]

    def _response(self, **kwargs) -> _ResponseT:
        """
        The global response handler for the data source, with automagic
        serialisation and deserialisation to/from JSON

        @kwargs URL route parameters
        @return HTTP response body
        """
        if not request.accept_mimetypes.accept_json:
            # Client must accept JSON
            return 'I only understand JSON', 406

        method = HTTPMethod(request.method)

        if _has_request_body(method):
            # Deserialise request body
            try:
                data = request.get_json(force=True)
                response = self._methods[method](data, **kwargs)

            except (BadRequest, ValueError):
                # If the JSON cannot be decoded (BadRequest), or the
                # handler can't make sense of the decoded data
                # (ValueError), then (re)raise a Bad Request response
                return 'Couldn\'t decode request body', 400

        else:
            response = self._methods[method](**kwargs)

        # Serialise and return
        serialised = json.dumps(response, separators=(',', ':'))
        return serialised, 200, {'Content-Type': 'application/json'}

class API(object):
    """ HTTP API building framework """
    def __init__(self, name:str):
        """
        @param  name  API service name
        """
        self._service = Flask(name)
        self._routes = {}

        # Secret shutdown route
        self._shutdown_route = '/{}'.format(uuid4().hex)

    def _shutdown_handler(self) -> str:
        """
        Initiate server shutdown request
        Based on http://flask.pocoo.org/snippets/67/
        """
        shutdown = request.environ.get('werkzeug.server.shutdown')
        if shutdown is None:
            raise RuntimeError('Not running with the Werkzeug server')
        shutdown()

        return 'API service shutting down...'

    def create_route(self, route:str) -> Endpoint:
        """
        Define a route; where routes may be parametrised, using tags
        within angled brackets
       
        @param  route  Route string for endpoint
        @return An Endpoint object

        Route parametrisation example:

           /path/to/<foo>

        Here the value of `foo` would be mapped into the respective
        keyword argument of the Endpoint's method handlers.
        """
        if route in self._routes:
            del self._routes[route]

        self._routes[route] = Endpoint(route)
        return self._routes[route]

    def listen(self, port:int=5000):
        """
        Build the API service, defined by its endpoints, and start it

        @param  port  The port to listen for HTTP requests
        """
        for route, source in self._routes.items():
            self._service.add_url_rule(
                rule=route,
                endpoint=route,
                view_func=source._response,
                methods=source._get_methods()
            )

        # Secret shutdown route
        self._service.add_url_rule(
            rule=self._shutdown_route,
            endpoint=self._shutdown_route,
            view_func=self._shutdown_handler,
            methods=['POST']
        )

        self._port = port
        self._running = True
        self._service.run(debug=False, host='0.0.0.0', port=port)

    def stop(self):
        """
        Stop the running service: We can only do this by making a
        request to a special endpoint :P
        """
        if self._running:
            conn = HTTPConnection('localhost', self._port)
            conn.request('POST', self._shutdown_route)
            conn.close()
            self._running = False
