"""
Generic Dependency Injector for Loggers
=======================================
Provides a generic method to inject logging into instantiated objects or
augment classes with the same logging

Exportable types:   `LoggingContext`, `LoggingFunctionClass`
Exportable classes: `LoggingFunction`, `RuntimeLogging`, `LoggingMapper`

LoggingContext
--------------
This is a convenience model for wrapping up the context of the logged
function and the state of the logging function. It contains the
following members that *must* not be mutated:

* `fn` The logged function
* `args` The positional arguments passed to the logged function
* `kwargs` The keyword arguments passed to the logged function
* `preexec` The logging functions preexecution return value
* `output` The output of the logged function, set by calling `exec`

The purpose of these values is to provide context to the logging,
specifically for determining the logging measure, value and any
associated metadata. You should never have to instantiate this class,
but the details of its members are necessary for any non-trivial
derivation (see below), plus the class can be used as a type hint.

LoggingFunction
---------------
`LoggingFunction` is the abstract base of the wrapper function that will
perform the logging on a given function. It defines four abstract
methods that need to be implemented:

* `preexec` This function is executed before the wrapped function, with
  the logging context, and its return value is passed back into said
  context. This should be used for any pre-execution setup and/or
  logging.

* `postexec` This function is executed after the wrapped function, with
  the logging context. This should be used for any post-execution setup
  and/or logging.

* `get_measure` This function is used to derive the logging measure from
  the logging context.

* `get_metadata` This function is used to derive any logging metadata
  from the logging context.

A convenience member function, `log`, can be used in `preexec` or
`postexec` to write to the log. This will automatically call the
`get_measure` and `get_metadata` functions with the provided context.

IMPORTANT: In implementations of the above functions in a multithreaded
context, it is up to you to maintain thread safety. The easiest thing to
do -- and this applies in general -- would be to avoid the problem by
not relying on stored state and instead using the function's arguments
and return values productively. In short: If you treat these as pure
functions (modulo any I/O done by the actual logging, of course), then
you'll be fine.

RuntimeLogging
--------------
`RuntimeLogging` is a partially implemented `LoggingFunction` that logs
the execution time of the logged function. A full implementation would
need to define the `get_measure` and `get_metadata` functions. This,
therefore, may be used as a base for specific implementations.

LoggingMapper
-------------
`LoggingMapper` allows you to map your class methods to logging
functions and then inject them into your class or object. It has the
following methods:

* `map_logging_to_method` Adds a logging function to a specific method

* `map_logging_to_abstract_methods` Adds a logging function to a given
  base class' abstract methods

* `map_logging_to_public_methods` Adds a logging function to a given
  class or object's "public" methods

* `inject_logging` Injects the logging methods into an instantiated
  object or, if used as a class decorator, the decorated class

Example
-------
Hopefully the following will illustrate usage:

    my_logger = SomeLoggerImplementation()

    class MyRuntimeLogging(RuntimeLogging):
        def get_measure(self, ctx):
            fn_name = ctx.fn.__name__
            return '{}_runtime'.format(fn_name)

        def get_metadata(self, ctx):
            return None

    my_logging_map = LoggingMapper(my_logger)

    my_logging_map.map_logging_to_public_methods(MyRuntimeLogging)

    # Inject into an already instantiated object
    my_logging_map.inject_logging(some_object)

    # ...or decorate a class with the logging
    # Will be of type SomeNewClassInjectedWithSomeLoggerImplementation
    @my_logging_map.inject_logging
    class SomeNewClass(Foo, Bar, Quux):
        # etc., etc.

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
import inspect
import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, Dict, Optional, Sequence, Union

from hgicommon.models import Model

from cookiemonster.logging.logger import Logger
from cookiemonster.logging.types import RecordableValue


class LoggingContext(Model):
    """ Encapsulate the logging wrapper context """
    def __init__(self, fn:Callable[..., Any], args:Optional[Sequence], kwargs:Optional[Dict[str, Any]]) -> None:
        # Function context
        # FIXME? Do bound methods need some reference to their instance?
        self.fn = fn
        self.name = fn.__name__
        self.args = args or []
        self.kwargs = kwargs or {}

        # Runtime context (set later)
        self.preexec = None  # type: Any
        self.output = None   # type: Any

    def exec(self):
        """ Execute function in context """
        self.output = self.fn(*self.args, **self.kwargs)


class LoggingFunction(metaclass=ABCMeta):
    """ Abstract logging function """
    def __init__(self, logger:Logger) -> None:
        self.logger = logger

    def __call__(self, fn:Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator to apply defined logging to a function

        @param   fn  Function to log
        @return  Function with logging function applied
        """
        @wraps(fn)
        def wrapper(*args, **kwargs):
            context = LoggingContext(fn, args, kwargs)
            context.preexec = self.preexec(context)
            context.exec()
            self.postexec(context)
            return context.output

        return wrapper

    def log(self, context:LoggingContext, values:Union[RecordableValue, Dict[str, RecordableValue]]):
        """ Convenience wrapper to self.logger.record """
        measured = self.get_measure(context)
        metadata = self.get_metadata(context)
        self.logger.record(measured, values, metadata)

    @abstractmethod
    def get_measure(self, context:LoggingContext) -> str:
        """ Derive the measured variable from the context """

    @abstractmethod
    def get_metadata(self, context:LoggingContext) -> Optional[Dict]:
        """ Derive the logged metadata, if any, from the context """

    @abstractmethod
    def preexec(self, context:LoggingContext) -> Any:
        """ Pre-execution logging function """

    @abstractmethod
    def postexec(self, context:LoggingContext):
        """ Post-execution logging function """


class RuntimeLogging(LoggingFunction, metaclass=ABCMeta):
    """ Log the time taken for function execution """
    def preexec(self, context:LoggingContext) -> Any:
        logging.debug('Log preexecution for %s', context.name)
        return time.monotonic()

    def postexec(self, context:LoggingContext):
        logging.debug('Log postexecution for %s', context.name)
        duration = time.monotonic() - context.preexec
        self.log(context, duration)


LoggingFunctionClass = type

class LoggingMapper(object):
    """ Logging to method mapping and dependency injector """
    def __init__(self, logger:Logger) -> None:
        self.logger = logger
        self.mapping = defaultdict(list)  # type: Dict[str, List[LoggingFunctionClass]]

    def map_logging_to_method(self, method:str, logging:LoggingFunctionClass):
        """
        Associate logging function with named method

        @param   method   Method name
        @param   logging  Logging function class
        """
        if inspect.isabstract(logging) or not issubclass(logging, LoggingFunction):
            raise TypeError('LoggingFunction implementation expected')

        self.mapping[method].append(logging)

    def map_logging_to_abstract_methods(self, abc:type, logging:LoggingFunctionClass):
        """
        Associate logging function with all abstract methods of an
        abstract base class

        @param   abc      Abstract base class to derive abstract methods
        @param   logging  Logging function class
        """
        if not inspect.isabstract(abc):
            raise TypeError('Abstract base class expected')

        for method in abc.__abstractmethods__:
            self.map_logging_to_method(method, logging)

    def map_logging_to_public_methods(self, target:Union[object, type], logging:LoggingFunctionClass):
        """
        Associate logging function with all public class/object methods

        @param   target   Class or object to derive public methods
        @param   logging  Logging function class
        """
        for method in dir(target):
            if callable(getattr(target, method)) and not method.startswith('_'):
                self.map_logging_to_method(method, logging)

    def inject_logging(self, target:Union[object, type]) -> Optional[type]:
        """
        Inject logging functions defined by the mapping into the given
        object or class. When used on an instantiated object, that
        object will be monkey-patched with said logging; when used
        against a class, as a class decorator, a new class will be
        generated.

        @param   target  Object or class into which to inject logging
        @return  Decorated class, if injecting into a class
                 None, if injecting into an instantiated object

        Note: When decorating a class, the decorated class name will be
        `XInjectedWithY`, where X is the original class name and Y is
        the logger class name.
        """
        decorating = False
        if inspect.isclass(target):
            decorating = True
            decorated_methods = {}  # type: Dict[str, Callable[..., Any]]

        for method, loggings in self.mapping.items():
            logged_fn = getattr(target, method)
            if not callable(logged_fn):
                raise TypeError('Cannot decorate uncallable attribute "{}"'.format(method))

            for logging_fn in loggings:
                logging.debug('Decorating %s with %s', method, logging_fn.__name__)
                logging_wrapper = logging_fn(self.logger)
                logged_fn = logging_wrapper(logged_fn)

            if decorating:
                decorated_methods[method] = logged_fn
            else:
                # FIXME? Does it matter that this is not a bound method?
                # It seems to work fine, but it feels wrong. Commit
                # a457ba3 correctly bound the methods, but then the
                # logging context function was incorrectly bound...
                setattr(target, method, logged_fn)

        if decorating:
            class_name = '{}InjectedWith{}'.format(target.__name__, self.logger.__class__.__name__)
            return type(class_name, (target,), decorated_methods)
