'''
Common Models
=============

Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''
import json
from datetime import date
from typing import Union, Dict, List

from hgicommon.models import Model
from cookiemonster.common.collections import Metadata

""" Metadata import type: Metadata, dictionary or (JSON) string """
_MetadataT = Union[Metadata, Dict[str, List[str]], dict, str]


class FileUpdate(Model):
    """
    Model of a file update.
    """
    def __init__(self, file_location: str, file_hash: hash, timestamp: date, metadata: _MetadataT):
        """
        Constructor.
        :param file_location: the location of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        :param metadata: the metadata of the file
        """
        self.file_location = file_location
        self.file_hash = file_hash
        self.timestamp = timestamp

        # FIXME? This is probably a teensy bit "unpythonic".
        # It will do for now...
        if type(metadata) is Metadata:
            self.metadata = metadata
        elif type(metadata) is dict:
            # Canonicalise dictionary into Metadata
            self.metadata = Metadata(metadata)
        elif type(metadata) is str:
            # Attempt to build Metadata from JSON
            self.metadata = Metadata(json.loads(metadata))
        else:
            raise TypeError('Could not parse metadata')
