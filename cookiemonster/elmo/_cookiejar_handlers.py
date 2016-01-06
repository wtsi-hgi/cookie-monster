'''
Cookie Jar API Handlers
=======================
Exportable classes: `CookieJarHandlers`, `QueueLength`

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

from typing import Optional
from hgicommon.models import Model
from cookiemonster.elmo._handlers import DependencyInjectionHandler

class QueueLength(Model):
    ''' Queue length model '''
    def __init__(self, size:Optional[int]=None):
        self.queue_length = size

class CookiePath(Model):
    ''' Cookie path model '''
    def __init__(self, path:Optional[str]=None):
        self.path = path

class CookieJarHandlers(DependencyInjectionHandler):
    ''' Handler functions for CookieJar '''
    def GET_queue_length(self, **kwargs):
        cookiejar = self._dependency
        return QueueLength(cookiejar.queue_length())

    def POST_mark_for_processing(self, cookie:CookiePath, **kwargs):
        cookiejar = self._dependency
        cookiejar.mark_for_processing(cookie.path)
        return cookie
