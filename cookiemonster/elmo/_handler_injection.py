'''
Dependency Injection Handler
============================
Exportable classes: `DependencyInjectionHandler`

DependencyInjectionHandler
--------------------------
Superclass for data source handlers that allow dependency injection
through the `inject_dependency`.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
'''

class DependencyInjectionHandler(object):
    ''' Dependency injection superclass for handlers '''
    def inject_dependency(self, dependency:object):
        self._dependency = dependency
