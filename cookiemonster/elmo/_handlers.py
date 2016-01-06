'''
Generic Handler Classes
=======================
Exportable classes: `DependencyInjectionHandler`

DependencyInjectionHandler
--------------------------
Superclass for data source handlers that allow dependency injection
through the constructor.

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
    def __init__(self, dependency:object):
        self._dependency = dependency
