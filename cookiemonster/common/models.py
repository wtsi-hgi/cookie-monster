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

from abc import ABCMeta
from datetime import date
from typing import Union, Dict, List
import json


class Model(metaclass=ABCMeta):
    """
    Abstract base class for models.
    """
    def __str__(self) -> str:
        string_builder = []
        for property, value in vars(self).items():
            string_builder.append("%s: %s" % (property, value))
        return "{ %s }" % ', '.join(string_builder)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for property, value in vars(self).items():
            if other.__dict__[property] != self.__dict__[property]:
                return False
        return True

class Metadata(dict):
    '''
    Self-canonicalising dictionary for metadata
    '''
    def __init__(self, base=None, **kwargs):
        '''
        Override constructor, so base and kwargs are canonicalised
        '''
        if base and type(base) is dict:
            for key, value in base.items():
                self.__setitem__(key, value)

        for key, value in kwargs.items():
            self.__setitem__(key, value)

    def __setitem__(self, key, value):
        '''
        Override __setitem__, so scalar values are put into a list and
        lists are sorted and made unique

        n.b., We assume our dictionaries are only one deep
        '''
        if type(value) is list:
            super().__setitem__(key, sorted(set(value)))

        else:
            super().__setitem__(key, [value])

''' Metadata import type: Metadata, dictionary or (JSON) string '''
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
