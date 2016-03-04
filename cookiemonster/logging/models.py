from numbers import Real
from typing import Union, Dict

from datetime import datetime
from hgicommon.models import Model


class Log(Model):
    """
    Model of a log that stores a measurement value with a name, taken at a particular moment in time.
    """
    def __init__(self, measurement_name: str, measurement_value: Union[str, Real], metadata: Dict=None,
                 timestamp: datetime=datetime.now()):
        self.measurement_name = measurement_name
        self.measurement_value = measurement_value
        self.metadata = metadata if metadata is not None else dict()
        self.timestamp = timestamp
