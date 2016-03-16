from abc import ABCMeta
from typing import Optional
from typing import TypeVar, Generic, Iterable

from hgicommon.data_source import RegisteringDataSource
from hgicommon.data_source.common import DataSourceType


class ResourceAccessor(metaclass=ABCMeta):
    """
    Accessor through which resources can be accessed. Implementations dictate the resources that are made available and
    the methods through which they are accessed.
    """

ResourceAccessType = TypeVar("ResourceAccessType", bound=ResourceAccessor)


class ResourceAccessorContainer(Generic[ResourceAccessType], metaclass=ABCMeta):
    """
    Container of a resource accessor.
    """
    def __init__(self):
        """
        Constructor.
        """
        self.resource_accessor = None  # type: Optional[ResourceAccessType]


class ResourceAccessorContainerRegisteringDataSource(RegisteringDataSource, metaclass=ABCMeta):
    """
    Registering data source of `ResourceAccessorContainer` instances.
    """
    def __init__(self, directory_location: str, data_type: type, resource_accessor: ResourceAccessor=None):
        """
        Constructor.
        :param directory_location: the location to monitor for registering data
        :param data_type: the type of data that will be registered (should be a subclass of `ResourceAccessorContainer`)
        :param resource_accessor: the resource accessor that should injected into the containers
        """
        super().__init__(directory_location, data_type)
        self.resource_accessor = resource_accessor

    def extract_data_from_file(self, file_path: str) -> Iterable[DataSourceType]:
        resource_accessor_containers = super().extract_data_from_file(file_path)
        if self.resource_accessor is not None:
            for resource_accessor_container in resource_accessor_containers:
                resource_accessor_container.resource_accessor = self.resource_accessor
        return resource_accessor_containers
