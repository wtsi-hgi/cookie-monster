from cookiemonster.common.resource_accessor import ResourceRequiringRegisteringDataSource, ResourceAccessor, \
    ResourceAccessorContainer


class StubResourceAccessor(ResourceAccessor):
    """
    Stub `ResourceAccessor`.
    """


class StubResourceAccessorContainer(ResourceAccessorContainer):
    """
    Stub `ResourceAccessorContainer`.
    """


class StubResourceRequiringRegisteringDataSource(ResourceRequiringRegisteringDataSource):
    """
    Stub `ResourceRequiringRegisteringDataSource`.
    """
    def is_data_file(self, file_path: str) -> bool:
        return True
