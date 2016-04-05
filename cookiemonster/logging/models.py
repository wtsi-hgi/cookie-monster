"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from datetime import datetime, timezone
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
        self.timestamp = timestamp if timestamp is not None else datetime.now(timezone.utc)
