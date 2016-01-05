'''
HTTP API Framework
==================
A minimal framework for building an HTTP-based API service, where
parametrised routes can be associated with models and arbitrary method
handlers

Exportable classes: `API`, `HTTPMethod` (enum), `HTTPSource`

API
---
`API` provides the scaffolding to build an HTTP API source, defined by
its routes and method handlers, using the following methods:

* `create_route` Create a route, optionally parametrised and associated
  with a data model (for automatic serialisation/deserialisation)

* `listen` Build the service defined by its routes and method handlers
  and start it, listening on the specified port

HTTPMethod
----------
Enumeration of supported HTTP methods

HTTPSource
----------
An `HTTPSource` encapsulates an API route, with associated method
handlers. The following methods are exposed:

* `set_method_handler` Add/update a method handler function

Method Chaining
---------------
Note that `API.create_route` and `HTTPSource.add_method_handler` both
return an `HTTPSource` object, which self-references the route in
question. As such, methods can be chained for convenience:

    api.create_route('/api/<foo>', FooModel)
       .set_method_handler(HTTPMethod.GET,  foo_get_handler)
       .set_method_handler(HTTPMethod.POST, foo_post_handler)

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

import re
import inspect
import collections
import json
from typing import Callable, List, Interable, Union, Tuple, Optional
from enum import Enum

from flask import Flask, request

from hgicommon.models import Model


# Type aliases
_ResponseT = Union[str, Tuple[str, int], Tuple[str, int, dict]]
HandlerT = Callable[..., Union[Model, Iterable[Model]]]


class HTTPMethod(Enum):
    ''' Supported HTTP method enumeration '''
    GET    = 'GET'
    POST   = 'POST'
    PUT    = 'PUT'
    DELETE = 'DELETE'


def _has_request_body(method:HTTPMethod) -> bool:
    '''
    @param  HTTP method
    @return Whether the above comes with a non-trivial request body
    '''
    return method in [HTTPMethod.POST, HTTPMethod.PUT]


class HTTPSource(object):
    ''' HTTP data source and method handlers '''
    def __init__(self, route:str, model:Optional[Model]=None):
        self._route   = route
        self._model   = model
        self._methods = {}

        # Extract any parameters from route
        self._parameters = list(set(re.findall('(?<=<)\w+(?=>)', route)))

    def set_method_handler(self, method:HTTPMethod, handler:HandlerT) -> HTTPSource:
        '''
        Add/update an HTTP method handler for the data source

        @param  method   The HTTP method
        @param  handler  The method handling function
        @return Itself (i.e., the HTTPSource object)
        '''
        handler_args = inspect.getfullargspec(handler)

        if len(self._parameters) and handler_args.varkw is None:
            # The handlers for a parametrised route must have kwargs
            raise TypeError('A parametrised route handler must accept keyword arguments')

        if has_request_body(method) and len(handler_args.args) != 1:
            # POST and PUT handlers *must* have one argument, as well as 
            # kwargs, to pass in the deserialised request body
            raise TypeError('A {} handler must have a single argument'.format(method.value))

        if method in self._methods:
            del self._methods[method]

        self._methods[method] = handler
        return self

    def _get_methods(self) -> List[str]:
        '''
        @return A list of the defined method strings
        '''
        return [method.value for method in self._methods.keys()]

    def _serialise(self, model:Model) -> str:
        '''
        Serialise the model to a JSON string

        @param  model  Model object
        @return JSON serialised string
        '''
        return json.dumps({k:v for k, v in vars(model).items()}, separators=(',', ':'))

    def _response(self, **kwargs) -> _ResponseT:
        '''
        The global response handler for the data source, with automagic
        serialisation and deserialisation to/from JSON

        @kwargs URL route parameters
        @return HTTP response body
        '''
        if not request.accept_mimetypes.accept_json:
            # Client must accept JSON
            return 'I only understand JSON', 406

        method = HTTPMethod(request.method)

        if has_request_body(method):
            # Deserialise request body
            model = self._model()
            data  = request.get_json(force=True)

            try:
                for k, v in data.items():
                    setattr(model, k, v)

            except:
                return 'Couldn\'t deserialise request body', 400

            response = self._methods[method](model, **kwargs)

        else:
            response = self._methods[method](**kwargs)

        # Serialise
        if isinstance(response, collections.Iterable):
            # Collection
            items = [self._serialise(item) for item in response]
            json  = '[{}]'.format(','.join(items))

        else:
            # Single item
            json = self._serialise(response)

        return json, 200, {'Content-Type': 'application/json'}


class API(object):
    ''' HTTP API building framework '''
    def __init__(self, name:str):
        '''
        @param  name  API service name
        '''
        self._service = Flask(name)
        self._routes  = {}

    def create_route(self, route:str, model:Optional[Model]=None) -> HTTPSource:
        '''
        Define a route; where routes may be parametrised, using tags
        within angled brackets, and be associated with a specific model.
       
        @param  route  Route string for data source
        @param  model  Model representing data
        @return An HTTPSource object

        Route parametrisation example:

           /path/to/<foo>

        Here the value of `foo` would be mapped into the respective
        keyword argument of the HTTPSource's method handlers.
        '''
        if route in self._routes:
            del self._routes[route]

        self._routes[route] = HTTPSource(route, model)
        return self._routes[route]

    def listen(self, port:int=5000):
        '''
        Build the API service, defined by its routes, and start it

        @param  port  The port to listen for HTTP requests
        '''
        for route in self._routes:
            self._service.route(route._route, methods=route._get_methods)(route._response)

        self._service.run(debug=False, port=port)
