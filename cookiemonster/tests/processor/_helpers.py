"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
import shutil
from unittest.mock import MagicMock

from hgicommon.data_source import SynchronisedFilesDataSource
from typing import Sequence

from multiprocessing import Semaphore

from hgicommon.data_source.static_from_file import FileSystemChange

from cookiemonster.cookiejar import CookieJar


def block_until_processed(cookie_jar: CookieJar, cookie_paths: Sequence[str],
                          expected_number_of_calls_to_mark_as_complete: int):
    """
    Puts the given cookies into the cookie jar and wait until they have been completed/marked for reprocessing.
    :param cookie_jar: the cookie jar to put cookies to process into
    :param cookie_paths: the cookie paths to process
    :param expected_number_of_calls_to_mark_as_complete: the number of calls expected to the Cookie jar's
    `mark_as_complete` method
    """
    if cookie_jar.queue_length() != 0:
        raise RuntimeError("Already cookies in the jar")

    mark_as_complete_semaphore = Semaphore(0)
    original_mark_as_complete = cookie_jar.mark_as_complete

    def mark_as_complete(path: str):
        mark_as_complete_semaphore.release()
        original_mark_as_complete(path)

    cookie_jar.mark_as_complete = MagicMock(side_effect=mark_as_complete)

    for cookie_path in cookie_paths:
        cookie_jar.mark_for_processing(cookie_path)

    calls_to_mark_as_complete = 0
    while calls_to_mark_as_complete != expected_number_of_calls_to_mark_as_complete:
        mark_as_complete_semaphore.acquire()
        calls_to_mark_as_complete += 1

    # Not rebinding `mark_as_complete` and `mark_as_reprocess` back to the originals in case they have been re-binded
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
