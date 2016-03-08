from numbers import Real
from typing import Union, Dict

from datetime import datetime
from hgicommon.models import Model


class Log(Model):
    """
    Model of a dated log that stores a measurement value with a name and optional metadata.
    """
    def __init__(self, measured: str, value: Union[str, Real], metadata: Dict=None,
                 timestamp: datetime=datetime.now()):
        self.measured = measured
        self.value = value
        self.metadata = metadata if metadata is not None else dict()
        self.timestamp = timestamp
