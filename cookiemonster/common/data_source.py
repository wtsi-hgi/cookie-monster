import collections
import copy
from abc import abstractmethod
from typing import Sequence, Iterable, TypeVar, Generic

SourceDataType = TypeVar('T')


# XXX: Making this abstract for some reason interferes with generics
class DataSource(Generic[SourceDataType]):
    """
    A source of instances of `SourceDataType`.
    """
    @abstractmethod
    def get_all(self) -> Sequence[SourceDataType]:
        """
        Gets the data aty the source
        :return: instances of `SourceDataType`
        """
        pass


class MultiDataSource(DataSource[SourceDataType]):
    """
    Aggregator of instances of data from multiple sources.
    """
    def __init__(self, sources: Iterable[DataSource]=()):
        """
        Constructor.
        :param sources: the sources of instances of `SourceDataType`
        """
        self.sources = copy.copy(sources)

    def get_all(self) -> Sequence[SourceDataType]:
        aggregated = []
        for source in self.sources:
            aggregated.extend(source.get_all())
        return aggregated


class StaticDataSource(DataSource[SourceDataType]):
    """
    Static source of data.
    """
    def __init__(self, data: Iterable[SourceDataType]):
        if not isinstance(data, collections.Iterable):
            raise ValueError("Data must be iterable")
        self._data = copy.copy(data)

    def get_all(self) -> Sequence[SourceDataType]:
        return self._data


class InFileDataSource(DataSource[SourceDataType]):
    """
    TODO
    """
    def __init__(self, directory_location: str):
        """
        Default constructor.
        :param directory_location: the location of the processor
        """
        self._directory_location = directory_location

    def get_all(self):
        pass

    def is_monitoring(self) -> bool:
        """
        Whether this monitor is monitoring.
        :return: state of the monitor
        """
        raise NotImplementedError()

    def start(self):
        """
        Starts monitoring processor in the directory in a new thread.
        """
        raise NotImplementedError()

    def stop(self):
        """
        Stops monitoring processor in the directory.
        """
        raise NotImplementedError()
