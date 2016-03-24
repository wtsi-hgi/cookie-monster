"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from hgicommon.data_source import register

from cookiemonster.tests.common.stubs import StubResourceAccessorContainer

register(StubResourceAccessorContainer())
register(StubResourceAccessorContainer())
register(StubResourceAccessorContainer())
