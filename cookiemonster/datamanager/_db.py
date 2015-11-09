'''
Database Abstraction
====================

foo...

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

import sqlite3

# TODO Probably easier to use SQLAlchemy for this. The schema is super
# simple and any idiot could build it by hand, but for the sake of
# decoupling...

# TODO Type hints

class DBException(Exception):
    ''' Database exception '''
    pass

class DB(object):
    ''' TODO Docstring '''
    def __init__(self, database):
        ''' Connect to database and check schema '''
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()
        self.check_schema()

    def __del__(self):
        ''' Close database connection '''
        self.connection.close()

    def check_schema(self):
        '''
        Check database schema
        '''
        pass
