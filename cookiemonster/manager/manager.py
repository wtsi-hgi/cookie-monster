'''
Data Manager
============

foo...

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from cookiemonster.common.models import FileUpdate
from cookiemonster.datamanager.db import DB

# FIXME Should this be a global, or rather passed as an argument?
_DATABASE_FILE = 'data.sqlite'

# TODO Type hints

class DataManager(object):
    ''' TODO Docstring '''
    def __init__(self):
        self.db = DB(_DATABASE_FILE)

    def __call__(self):
        ''' Syntactic sugar '''
        return self.produce()

    # TODO Better name? import?
    def consume(self, models):
        '''
        Add the supplied list of models to the database, ready for
        processing
        '''
        pass

    # TODO Better name? get?
    def produce(self):
        '''
        Return a new FileUpdate model for processing, or None if we've
        exhausted the pool.
        '''
        pass
