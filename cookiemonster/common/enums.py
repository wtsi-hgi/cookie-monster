"""
Common Enumerations
===================

Note that enum values are for the sake of persistent storage and should
be unique.

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
"""

from enum import Enum


class EnrichmentSource(Enum):
    IRODS_UPDATE = "irods_update"
    IRODS = "irods"
    SEQUENCE_SCAPE = "sequencescape"
    FILE_HEADER = "fileheader"
    # TODO? Expand this list appropriately
