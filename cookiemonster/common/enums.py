'''
Common Enumerations
===================

MetadataNS
----------
This is a two-tier enumeration used to namespace attributes from
different sources and appropriate sections

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''
from enum import Enum

class MetadataNS(object):
    class IRODS(Enum):
        FileSystem = 'irods:fs'
        AVUs       = 'irods:avu'
        ACLs       = 'irods:acl'

    class SequenceScape(Enum):
        TODO       = 'sequencescape:todo'

    class FileHeader(Enum):
        TODO       = 'header:todo'