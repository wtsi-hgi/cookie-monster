'''
Exception Catching Decorator
============================
A class decorator for `CookieJar`s that will automatically retry any
method that raises an exception, in some hope that it'll work the next
time

Exportable functions: `too_big_to_fail`
Exportable classes: `MaxAttemptsExhausted`

`too_big_to_fail`
----------------
When applied to a class, all `CookieJar` methods will be wrapped into a
`try...catch` block that, if the method raises an exception, will retry
that method again and again. To avoid any potential futility, the
maximum number of retries can be set via an additional keyword argument
to a decorated class' constructor (`max_attempts:int`); if omitted, it
will default to retrying ad infinitum.

For example, to create a seemingly invincible version of a `CookieJar`
implementation called, say, `ElmosOreos`, you would do something like
the following:

    @too_big_to_fail
    class InvincibleElmosOreos(ElmosOreos):
        pass

**WARNING** Successive calls to any function that manipulates state
non-idempotently before failing may cause unintended consequences and
corruption of state!

`MaxAttemptsExhausted`
----------------------
This exception will be raised if the maximum number of attempts (if set)
is exhausted without the `CookieJar` method executing without error.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''
from typing import Any, Callable, Optional

from cookiemonster.cookiejar import CookieJar


_cookie_jar_methods = CookieJar.__abstractmethods__


class MaxAttemptsExhausted(Exception):
    pass


def too_big_to_fail(cookiejar:CookieJar) -> CookieJar:
    ''' Decorator to catch and retry all CookieJar methods that raise '''
    class _invincible(cookiejar):
        def __init__(self, *args, **kwargs):
            if 'max_attempts' in kwargs:
                max_attempts = kwargs['max_attempts']
                self._can_attempt = lambda x: x < max_attempts
                del kwargs['max_attempts']
            else:
                self._can_attempt = lambda _: True

            super().__init__(*args, **kwargs)

            # Monkey-patch CookieJar methods with exception catching decorator
            for method in _cookie_jar_methods:
                setattr(self.__class__, method, self._catcher(getattr(cookiejar, method)))

        def _catcher(self, fn:Callable[..., Any]) -> Callable[..., Any]:
            '''
            Decorator that catches all exceptions

            @param   fn  Function to decorate
            @return  function
            '''
            def wrapper(cls, *args, **kwargs):
                completed = False
                output = None
                attempts = 0

                while not completed and self._can_attempt(attempts):
                    try:
                        output = fn(cls, *args, **kwargs)
                        completed = True
                    except:
                        attempts += 1

                if not completed:
                    raise MaxAttemptsExhausted()

                return output

            return wrapper

    return _invincible
