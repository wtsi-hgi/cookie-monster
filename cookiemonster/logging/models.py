from numbers import Real
from typing import Union, Dict

from datetime import datetime
from hgicommon.models import Model


class Log(Model):
    """
    Model of a dated log that stores a measurement value with a name and optional metadata.
    """
    def __init__(self, measuring: str, value: Union[str, Real], metadata: Dict=None,
                 timestamp: datetime=datetime.now()):
        self.measuring = measuring
        self.value = value
        self.metadata = metadata if metadata is not None else dict()
        self.timestamp = timestamp
