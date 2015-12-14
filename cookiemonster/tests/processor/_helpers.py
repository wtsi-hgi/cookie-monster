import shutil
from hgicommon.data_source import SynchronisedFilesDataSource
from typing import Sequence

from multiprocessing import Semaphore

from hgicommon.data_source.static_from_file import FileSystemChange


def add_data_files(source: SynchronisedFilesDataSource, data_files: Sequence[str]):
    """
    Copies the given data files to the folder monitored by the given synchronised files data source. Blocks until
    all the files have been processed by the data source.
    :param source: the data source monitoring a folder
    :param data_files: the data files to copy
    """
    load_semaphore = Semaphore(0)

    def on_load(change: FileSystemChange):
        if change == FileSystemChange.CREATE:
            load_semaphore.release()

    source.add_listener(on_load)

    for data_file in data_files:
        shutil.copy(data_file, source._directory_location)

    loaded = 0
    while loaded != len(data_files):
        load_semaphore.acquire()
        loaded += 1

    source.remove_listener(on_load)
