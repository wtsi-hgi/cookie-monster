"""
Exception Catching Decorator
============================
A class decorator for abstract base classes that will automatically
retry any method that raises an exception, in some vain hope that it'll
work the next time

Exportable functions: `too_big_to_fail`
Exportable classes: `MaxAttemptsExhausted`

`too_big_to_fail`
----------------
When applied to derivatives of an abstract base class, all abstract
methods will be wrapped into a `try...catch` block that, if the method
raises any suppressed exception -- defined in the arguments to the
decorator -- it will retry that method again and again. To avoid any
potential futility, the maximum number of retries can be set via an
additional keyword argument to a decorated class' constructor
(`max_attempts:Optional[int]`); if omitted, it will default to retrying
ad infinitum.

For example, to create a seemingly invincible version of a `CookieJar`
implementation called, say, `ElmosOreos`, you would do something like
the following:

    @too_big_to_fail()
    class InvincibleElmosOreos(ElmosOreos):
        pass

...or if you only wanted to suppress certain errors:

    @too_big_to_fail(ConnectionError, TimeoutError):
    class JustKeepHammering(SomeClass):
        pass

**WARNING** Successive calls to any function that manipulates state
non-idempotently before failing may cause unintended consequences and
corruption of state!

`MaxAttemptsExhausted`
----------------------
This exception will be raised if the maximum number of attempts (if set)
is exhausted without the method executing successfully.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from typing import Any, Callable


class MaxAttemptsExhausted(Exception):
    pass


def too_big_to_fail(*suppressed:Exception) -> Callable[['Class'], 'Class']:
    if not suppressed:
        # ...then suppress everything
        suppressed = Exception,

    def decorator(base:'Class') -> 'Class':
        """
        Decorator to catch and retry all abstract methods that raise
        any suppressed exceptions
        """
        def _catcher(fn:Callable[..., Any]) -> Callable[..., Any]:
            """
            Decorator that catches suppressed exceptions

            @param   fn  Function to decorate
            @return  Catching function
            """
            def wrapper(obj, *args, **kwargs):
                completed = False
                output = None
                attempts = 0

                while not completed and obj._can_attempt(attempts):
                    try:
                        output = fn(obj, *args, **kwargs)
                        completed = True
                    except suppressed:
                        attempts += 1

                if not completed:
                    raise MaxAttemptsExhausted()

                return output

            return wrapper

        class _invincible(base):
            def __init__(self, *args, **kwargs):
                if 'max_attempts' in kwargs:
                    max_attempts = kwargs['max_attempts']
                    self._can_attempt = lambda x: x < max_attempts
                    del kwargs['max_attempts']
                else:
                    self._can_attempt = lambda _: True

                # Determine abstract methods
                abstract_methods = set()
                for cls in base.mro():
                    if hasattr(cls, '__abstractmethods__'):
                        abstract_methods = abstract_methods.union(cls.__abstractmethods__)

                # Monkey-patch abstract methods with exception catching decorator
                for method in abstract_methods:
                    setattr(self.__class__, method, _catcher(getattr(base, method)))

                super().__init__(*args, **kwargs)

        return _invincible

    return decorator
