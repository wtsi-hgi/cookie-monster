"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from datetime import datetime
from typing import Union, Dict

from hgicommon.models import Model

from cookiemonster.logging.types import RecordableValue


class Log(Model):
    """
    Model of a dated log that stores a measurement value with a name and optional metadata.
    """
    def __init__(self, measured: str, values: Union[RecordableValue, Dict[str, RecordableValue]], metadata: Dict=None,
                 timestamp: datetime=None):
        if not isinstance(values, Dict):
            self.value = values
            values = {"value": values}

        self.measured = measured
        self.values = values
        self.metadata = metadata if metadata is not None else dict()
        self.timestamp = timestamp if timestamp is not None else datetime.now()
