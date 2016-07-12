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

Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

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
from functools import wraps
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
            @wraps(fn)
            def wrapper(cls, *args, **kwargs):
                with self._request_semaphore:
                    return fn(cls, *args, **kwargs)

            return wrapper

    return _rate_limited
