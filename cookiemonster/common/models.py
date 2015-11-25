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
<<<<<<< HEAD
from typing import Union, Dict, List, Any
=======
from typing import Optional, Union, Dict, List

>>>>>>> f8eca9da059e490b0ba6ff869f3edbb74ddd8796
from hgicommon.collections import Metadata
from hgicommon.models import Model

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


class FileProcessState(Model):
    '''
    Model file processing state
    '''
    def __init__(self, current_state: FileUpdate, processed_state: Optional[FileUpdate]):
        '''
        Constructor

        @param  current_state   Current FileUpdate for processing
        @param  processed_state Previously processed FileUpdate
        '''
        self.current_state   = current_state
        self.processed_state = processed_state

        # TODO? Generate diff...



class Notification(Model):
    """
    A model of a notification that should be sent to an external process.
    """
    def __init__(self, external_process_name: str, data: Any=None):
        """
        Default constructor.
        :param external_process_name: the name of the external process that should be informed
        :param data: the data (if any) that should be given to the external process
        """
        raise NotImplementedError()
