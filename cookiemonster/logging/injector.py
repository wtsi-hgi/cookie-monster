"""
Generic Dependency Injector for Loggers
=======================================
Provides a generic method to inject logging into instantiated objects or
augment classes with the same logging

Exportable types:   `FunctionContext`, `LoggingFunctionClass`
Exportable classes: `LoggingFunction`, `RuntimeLogging`, `LoggingMapper`

LoggingFunction
---------------
TODO

RuntimeLogging
--------------
TODO

LoggingMapper
-------------
`LoggingMapper` allows you to map your class methods to logging
functions and then inject them into your class or object. It has the
following methods:

* `add_logging_to_method` Adds a logging function to a specific method

* `add_logging_to_abstract_methods` Adds a logging function to a given
  class' abstract methods

* `add_logging_to_public_methods` Adds a logging function to a given
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
from abc import ABCMeta, abstractmethod
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from hgicommon.models import Model

from cookiemonster.logging.logger import Logger
from cookiemonster.logging.types import RecordableValue


class FunctionContext(Model):
    """ Encapsulate a wrapped function's context """
    def __init__(self, fn:Callable[..., Any], args, kwargs) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


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
            context = FunctionContext(fn, args, kwargs)
            self.preexec(context)
            output = fn(*args, **kwargs)
            self.postexec(context)
            return output

        return wrapper

    def log(self, context:FunctionContext, values:Union[RecordableValue, Dict[str, RecordableValue]]):
        """ Convenience wrapper to self.logger.record """
        measured = self.get_measure(context)
        metadata = self.get_metadata(context)
        self.logger.record(measured, values, metadata)

    @abstractmethod
    def get_measure(self, context:FunctionContext) -> str:
        """ Derive the measured variable from the function context """

    @abstractmethod
    def get_metadata(self, context:FunctionContext) -> Optional[Dict]:
        """ Derive the logged metadata, if any, from the function context """

    @abstractmethod
    def preexec(self, context:FunctionContext):
        """ Pre-execution logging function """

    @abstractmethod
    def postexec(self, context:FunctionContext):
        """ Post-execution logging function """


class RuntimeLogging(LoggingFunction, metaclass=ABCMeta):
    """ Log the time taken for function execution """
    def preexec(self, context:FunctionContext):
        start = time.monotonic()

    def postexec(self, context:FunctionContext):
        duration = time.monotonic() - self.start
        self.log(context, duration)


LoggingFunctionClass = type

class LoggingMapper(object):
    """ TODO """
    def __init__(self, logger:Logger) -> None:
        pass

    def add_logging_to_method(self, method:str, logging:LoggingFunctionClass):
        """
        Associate logging function with named method

        @param   method   Method name
        @param   logging  Logging function class
        """
        pass

    def add_logging_to_abstract_methods(self, abc:ABCMeta, logging:LoggingFunctionClass):
        """
        Associate logging function with all abstract methods of an
        abstract base class

        @param   abc      Abstract base class to derive abstract methods
        @param   logging  Logging function class
        """
        for method in abc.__abstractmethods__:
            self.add_logging_to_method(method, logging)

    def add_logging_to_public_methods(self, cls:type, logging:LoggingFunctionClass):
        """
        Associate logging function with all public class methods

        @param   cls      Class to derive public methods
        @param   logging  Logging function class
        """
        for method in dir(cls):
            if callable(getattr(cls, method)) and not method.startswith('_'):
                self.add_logging_to_method(method, logging)

    def inject_logging(self, target:Union[object, type]) -> Optional[type]:
        pass
