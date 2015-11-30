'''
Common Enumerations
===================

MetadataNS
----------
This is a two-tier enumeration used to namespace attributes from
different sources and appropriate sections

ProcessingQueueStates
---------------------
The potential states in which `Cookie`s can find themselves in on the
processing queue [cookie jar]

EnrichmentSource
----------------
The metadata enrichment sources (e.g., iRODS, SequenceScape, etc.)

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
    class iRODS(Enum):
        FileSystem = 'irods:fs'
        AVUs       = 'irods:avu'
        ACLs       = 'irods:acl'

    class SequenceScape(Enum):
        TODO       = 'sequencescape:todo'

    class FileHeader(Enum):
        TODO       = 'header:todo'


class ProcessingQueueState(Enum):
    TODO = 'TODO'


class EnrichmentSource(Enum):
    iRODS = 'irods'
    TODO  = 'todo'
