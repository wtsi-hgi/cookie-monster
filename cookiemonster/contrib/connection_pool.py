"""
Configurable HTTP Connection Pool
=================================
Patch the urllib3 HTTP connection pool, used by Requests in pycouchdb,
such that the pool settings can be changed.

Legalese
--------
Copyright (c) 2014 Michael Scharf
Copyright (c) 2016 Genome Research Ltd.

Authors:
* Michael Scharf <http://stackoverflow.com/a/22253656/876937>
* Christopher Harrison <ch12@sanger.ac.uk>

This file is part of Cookie Monster.
"""

def patch_http_connection_pool(**constructor_kwargs):
    """
    Override the default parameters of the HTTPConnectionPool
    constructor
    """
    from requests.packages.urllib3 import connectionpool, poolmanager

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super().__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme['http'] = MyHTTPConnectionPool
