from abc import ABCMeta
from typing import Optional
from typing import TypeVar, Generic, Iterable

from hgicommon.data_source import RegisteringDataSource
from hgicommon.data_source.common import DataSourceType


class ResourceAccessor(metaclass=ABCMeta):
    """
    TODO
    """

ResourceAccessType = TypeVar("ResourceAccessType", bound=ResourceAccessor)


class ResourceAccessorContainer(Generic[ResourceAccessType], metaclass=ABCMeta):
    """
    TODO
    """
    def __init__(self):
        """
        Constructor.
        """
        self.resource_accessor = None  # type: Optional[ResourceAccessType]


class ResourceRequiringRegisteringDataSource(RegisteringDataSource, metaclass=ABCMeta):
    """
    TODO
    """
    def __init__(self, directory_location: str, data_type: type, resource_accessor: ResourceAccessor=None):
        """
        Constructor.
        :param directory_location:
        :param data_type:
        :param resource_accessor:
        """
        super().__init__(directory_location, data_type)
        self.resource_accessor = resource_accessor

    def extract_data_from_file(self, file_path: str) -> Iterable[DataSourceType]:
        resource_accessor_containers = super().extract_data_from_file(file_path)
        if self.resource_accessor is not None:
            for resource_accessor_container in resource_accessor_containers:
                resource_accessor_container.resource_accessor = self.resource_accessor
        return resource_accessor_containers
