"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

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
import logging
import shutil
import unittest
from multiprocessing import Semaphore
from typing import Dict
from typing import Sequence, List, Iterable
from unittest.mock import MagicMock

from hgicommon.data_source import SynchronisedFilesDataSource
from hgicommon.data_source.static_from_file import FileSystemChange

from cookiemonster.cookiejar import CookieJar
from cookiemonster.processor.models import EnrichmentLoader
from cookiemonster.processor.models import Rule


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
        assert cookie_jar.mark_as_complete.call_count <= expected_number_of_calls_to_mark_as_complete
        calls_to_mark_as_complete += 1

    assert calls_to_mark_as_complete == cookie_jar.mark_as_complete.call_count

    # Not rebinding `mark_as_complete` and `mark_as_reprocess` back to the originals in case they have been re-binded
    # again since this method was called.


def add_data_files(source: SynchronisedFilesDataSource, data_files: Sequence[str]):
    """
    Copies the given data files to the folder monitored by the given synchronised files data source. Blocks until
    all the files have been processed by the data source. Assumes all data files register one item of data.
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


def _generate_cookie_ids(number: int) -> List[str]:
    """
    Generates the given number of example cookie ids.
    :param number: the number of example cookie ids to generate
    :return: the generated cookie ids
    """
    return ["/my/cookie/%s" % i for i in range(number)]


class _Checker:
    """
    Base class for checkers that use a test case for making assertions.
    """
    def __init__(self, test_case: unittest.TestCase):
        """
        Constructor.
        :param test_case: the test case through which assertions can be made
        """
        self._test_case = test_case


class RuleChecker(_Checker):
    """
    Checks assertions on rules.
    """
    def __init__(self, test_case: unittest.TestCase, rules: Iterable[Rule]):
        """
        Constructor.
        :param test_case: the test case through which assertions can be made
        :param rules: the rules that can be checked. Rules must have their underlying `matches` and `action` methods
        wrapped in a `MagicMock`, e.g. `MagicMock(side_effct=_action)`
        """
        super().__init__(test_case)
        self._rules = dict()  # type: Dict[str, Rule]
        for rule in rules:
            assert rule.name not in self._rules
            self._rules[rule.name] = rule

    def assert_call_counts(self, name: str, precondition_call_count: int, action_call_count: int):
        """
        Assert that the rule with the given name has the given call counts.
        :param name: the name of the rule
        :param precondition_call_count: the number of calls that should have been made to the given rule's `matches`
        method
        :param action_call_count: the number of calls that should have been made to the given rule's `action` method
        """
        rule = self._rules[name]
        self._test_case.assertEqual(rule._precondition.call_count, precondition_call_count)
        self._test_case.assertEqual(rule._action.call_count, action_call_count)


class EnrichmentLoaderChecker(_Checker):
    """
    Checks assertions on enrichment loaders.
    """
    def __init__(self, test_case: unittest.TestCase, enrichment_loaders: Iterable[EnrichmentLoader]):
        """
        Constructor.
        :param test_case: the test case through which assertions can be made
        :param enrichment_loaders: the enrichment loaders that can be checked. Enrichment loaders must have their
        underlying `can_enrich` and `load_enrichment` methods wrapped in a `MagicMock`, e.g.
        `MagicMock(side_effct=_can_enrich)`
        """
        super().__init__(test_case)
        self._enrichment_loaders = dict()  # type: Dict[str, EnrichmentLoader]
        for enrichment_loader in enrichment_loaders:
            assert enrichment_loader.name not in self._enrichment_loaders
            self._enrichment_loaders[enrichment_loader.name] = enrichment_loader

    def assert_call_counts(self, name: str, can_enrich_call_count: int, load_enrichment_call_count: int):
        """
        Assert that the enrichment loader with the given name has the given call counts.
        :param name: the name of the enrichment loader
        :param can_enrich_call_count: the number of calls that should have been made to the given enrichment loader's
        `can_enrich` method
        :param load_enrichment_call_count: the number of calls that should have been made to the given enrichment
        loader's `load_enrichment` method
        """
        enrichment_loader = self._enrichment_loaders[name]
        self._test_case.assertEqual(enrichment_loader._can_enrich.call_count, can_enrich_call_count)
        self._test_case.assertEqual(enrichment_loader._load_enrichment.call_count, load_enrichment_call_count)
