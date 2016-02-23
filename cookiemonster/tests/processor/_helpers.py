import shutil
from unittest.mock import MagicMock

from hgicommon.data_source import SynchronisedFilesDataSource
from typing import Sequence

from multiprocessing import Semaphore

from hgicommon.data_source.static_from_file import FileSystemChange

from cookiemonster.cookiejar import CookieJar


def block_until_processed(cookie_jar: CookieJar, cookie_paths: Sequence[str]):
    """
    Puts the given cookies into the cookie jar and wait until they have been completed/marked for reprocessing.
    :param cookie_jar: the cookie jar to put cookies to process into
    :param cookie_paths: the cookie paths to process
    """
    if cookie_jar.queue_length() != 0:
        raise RuntimeError("Already cookies in the jar")

    processed_semaphore = Semaphore(0)

    original_mark_as_completed = cookie_jar.mark_as_complete
    original_mark_as_reprocess = cookie_jar.mark_for_processing

    def on_complete(path: str):
        processed_semaphore.release()
        original_mark_as_completed(path)

    def on_reprocess(path: str):
        processed_semaphore.release()
        original_mark_as_reprocess(path)

    cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)
    cookie_jar.mark_for_processing = MagicMock(side_effect=on_reprocess)

    for cookie_path in cookie_paths:
        cookie_jar.mark_for_processing(cookie_path)

    processed = 0
    while processed != len(cookie_paths):
        processed_semaphore.acquire()
        processed += 1

    # Not rebinding `mark_as_complete` and `mark_as_reprocess` back to the originals in case they have been rebinded
    # again since this method was called.


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
