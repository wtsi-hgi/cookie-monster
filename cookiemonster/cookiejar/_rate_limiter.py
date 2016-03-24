"""
Rate Limiter Decorator
======================
A class decorator for `CookieJar`s that implements rate-limiting

Exportable functions: `rate_limited`

`rate_limited`
--------------
When applied to a class, all `CookieJar` methods will be rate-limited.
This is controlled via an additional argument to the constructor
(`max_requests_per_second:int`).

For example, to create a rate-limited version of a `CookieJar`
implementation called, say, `BigBirdsBiscuits`, you would do something
like the following:

    @rate_limited
    class RateLimitedBigBirdsBiscuits(BigBirdsBiscuits):
        pass

`RateLimitedBigBirdsBiscuits` would now be instantiated with:

    my_cookies = RateLimitedBigBirdsBiscuits(max_req_per_sec, ...)

...where `...` indicate the arguments usually passed to
`BigBirdsBiscuits`'s constructor.

Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from threading import Semaphore, Timer
from typing import Any, Callable

from cookiemonster.cookiejar import CookieJar


_cookie_jar_methods = CookieJar.__abstractmethods__


class _RateLimitedSemaphore(Semaphore):
    """
    Semaphore that takes a second to release.
    """
    def release(self):
        Timer(1.0, super().release).start()


def rate_limited(cookiejar:CookieJar) -> CookieJar:
    """ Decorator to apply rate limiting on all CookieJar methods """
    class _rate_limited(cookiejar):
        def __init__(self, max_requests_per_second:int, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._request_semaphore = _RateLimitedSemaphore(max_requests_per_second)

            # Monkey-patch CookieJar methods with limiter decorator
            for method in _cookie_jar_methods:
                setattr(self.__class__, method, self._limiter(getattr(cookiejar, method)))

        def _limiter(self, fn:Callable[..., Any]) -> Callable[..., Any]:
            """
            Decorator that rate-limits a function

            @param   fn  Function to decorate
            @return  Rate-limited function
            """
            def wrapper(cls, *args, **kwargs):
                with self._request_semaphore:
                    return fn(cls, *args, **kwargs)

            return wrapper

    return _rate_limited
