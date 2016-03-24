"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from numbers import Real
from typing import TypeVar

RecordableValue = TypeVar("ValueType", str, Real, None)
