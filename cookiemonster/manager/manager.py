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
        return self.get()

    # TODO Better name? import?
    def add(self, models):
        '''
        Add the supplied list of models to the database, ready for
        processing
        '''
        pass

    # TODO Better name? get?
    def get(self):
        '''
        Return a new FileUpdate model for processing, or None if we've
        exhausted the pool.
        '''
        # TODO: Logic to ensure only the latest update to a file is set to be processed
        pass

    # TODO: public interface to allow changes to the state of the work (complete(x), re-process(path), re-process-all())

    # TODO: get all previous changes to file x
