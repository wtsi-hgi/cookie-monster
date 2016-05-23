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
* `output` The output of the logged function

The purpose of these values is to provide context to the logging,
specifically for determining the logging measure, value and any
associated metadata.

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
  class' abstract methods

* `map_logging_to_public_methods` Adds a logging function to a given
  class' "public" methods

* `inject_logging` Injects the logging methods into an instantiated
  object or, if used as a class decorator, the decorated class

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
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from functools import wraps
from types import MappingProxyType
from typing import Any, Callable, Dict, Optional, Sequence, Union

from hgicommon.models import Model

from cookiemonster.logging.logger import Logger
from cookiemonster.logging.types import RecordableValue


class LoggingContext(Model):
    """ Encapsulate the logging wrapper context """
    def __init__(self, fn:Callable[..., Any], args:Optional[Sequence], kwargs:Optional[Dict[str, Any]]) -> None:
        # Function context
        self.fn = fn
        self.args = args
        self.kwargs = MappingProxyType(kwargs) if kwargs else None

        # Runtime context (set later)
        self.preexec = None  # type: Any
        self.output = None   # type: Any


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
            context.output = fn(*args, **kwargs)
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
        return time.monotonic()

    def postexec(self, context:LoggingContext):
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

    def map_logging_to_public_methods(self, cls:type, logging:LoggingFunctionClass):
        """
        Associate logging function with all public class methods

        @param   cls      Class to derive public methods
        @param   logging  Logging function class
        """
        if not inspect.isclass(cls):
            raise TypeError('Class expected')

        for method in dir(cls):
            if callable(getattr(cls, method)) and not method.startswith('_'):
                self.map_logging_to_method(method, logging)

    def inject_logging(self, target:Union[object, type]) -> Optional[type]:
        pass
