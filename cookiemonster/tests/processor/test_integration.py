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
from os.path import normpath, join, dirname, realpath
from tempfile import mkdtemp
from typing import Dict
from typing import Iterable
from unittest.mock import MagicMock

from cookiemonster.processor._enrichment import EnrichmentLoaderSource
from cookiemonster.processor._rules import RuleSource
from cookiemonster.processor.basic_processing import BasicProcessorManager
from cookiemonster.processor.models import Rule, EnrichmentLoader
from cookiemonster.tests.common.stubs import StubContext
from cookiemonster.tests.processor._enrichment_loaders.hash_loader import HASH_ENRICHMENT_LOADER_ID
from cookiemonster.tests.processor._enrichment_loaders.name_match_loader import NAME_ENRICHMENT_LOADER_MATCH_COOKIE, \
    NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID
from cookiemonster.tests.processor._enrichment_loaders.no_loader import NO_LOADER_ENRICHMENT_LOADER_ID
from cookiemonster.tests.processor._helpers import add_data_files, block_until_processed, _generate_cookie_ids, \
    RuleChecker, EnrichmentLoaderChecker
from cookiemonster.tests.processor._mocks import create_magic_mock_cookie_jar
from cookiemonster.tests.processor._rules.all_match_rule import ALL_MATCH_RULE_ID
from cookiemonster.tests.processor._rules.match_if_enriched_rule import HASH_ENRICHED_MATCH_RULE_ID
from cookiemonster.tests.processor._rules.name_match_rule import NAME_RULE_MATCH_COOKIE, NAME_MATCH_RULE_ID
from cookiemonster.tests.processor._rules.no_match_rule import NO_MATCH_RULE_ID

_RULE_FILE_LOCATIONS = [
    normpath(join(dirname(realpath(__file__)), "_rules/all_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "_rules/no_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "_rules/name_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "_rules/match_if_enriched_rule.py"))
]
_ENRICHMENT_LOADER_LOCATIONS = [
    normpath(join(dirname(realpath(__file__)), "_enrichment_loaders/name_match_loader.py")),
    normpath(join(dirname(realpath(__file__)), "_enrichment_loaders/no_loader.py")),
    normpath(join(dirname(realpath(__file__)), "_enrichment_loaders/hash_loader.py"))
]


class TestIntegration(unittest.TestCase):
    """
    Integration tests for processor.
    """
    _NUMBER_OF_COOKIES = 250
    _NUMBER_OF_PROCESSORS = 10

    def setUp(self):
        self.rules_directory = mkdtemp(prefix="rules", suffix=TestIntegration.__name__)
        self.enrichment_loaders_directory = mkdtemp(prefix="enrichment_loaders", suffix=TestIntegration.__name__)

        self.resource_accessor = StubContext()

        # Setup enrichment
        self.enrichment_loader_source = EnrichmentLoaderSource(
            self.enrichment_loaders_directory, self.resource_accessor)
        self.enrichment_loader_source.start()

        # Setup cookie jar
        self.cookie_jar = create_magic_mock_cookie_jar()

        # Setup rules source
        self.rules_source = RuleSource(self.rules_directory, self.resource_accessor)
        self.rules_source.start()

        # Setup the data processor manager
        self.processor_manager = BasicProcessorManager(
            self.cookie_jar, self.rules_source, self.enrichment_loader_source)

        def cookie_jar_connector(*args):
            self.processor_manager.process_any_cookies()

        self.cookie_jar.add_listener(cookie_jar_connector)

    def tearDown(self):
        shutil.rmtree(self.rules_directory)
        shutil.rmtree(self.enrichment_loaders_directory)

    def test_with_no_rules_or_enrichments(self):
        cookie_ids = _generate_cookie_ids(TestIntegration._NUMBER_OF_COOKIES)
        block_until_processed(self.cookie_jar, cookie_ids, TestIntegration._NUMBER_OF_COOKIES)

        self.assertEqual(self.cookie_jar.mark_as_complete.call_count, len(cookie_ids))
        self.cookie_jar.mark_as_failed.assert_not_called()

        # TODO: Call if no rules match and no further enrichments?

    @unittest.skip("Flaky test")
    def test_with_no_rules_but_enrichments(self):
        add_data_files(self.enrichment_loader_source, _ENRICHMENT_LOADER_LOCATIONS)

        TestIntegration._NUMBER_OF_COOKIES = 1
        cookie_ids = _generate_cookie_ids(TestIntegration._NUMBER_OF_COOKIES)
        cookie_ids.append(NAME_ENRICHMENT_LOADER_MATCH_COOKIE)
        expected_number_of_times_processed = len(cookie_ids) * 2
        block_until_processed(self.cookie_jar, cookie_ids, expected_number_of_times_processed)

        self.assertEqual(self.cookie_jar.mark_as_complete.call_count, expected_number_of_times_processed)
        self.cookie_jar.mark_as_failed.assert_not_called()

        enrichment_loader_checker = EnrichmentLoaderChecker(self, self.enrichment_loader_source.get_all())
        enrichment_loader_checker.assert_call_counts(
            NO_LOADER_ENRICHMENT_LOADER_ID, expected_number_of_times_processed, 0)
        enrichment_loader_checker.assert_call_counts(
            HASH_ENRICHMENT_LOADER_ID, expected_number_of_times_processed - 1, len(cookie_ids))
        enrichment_loader_checker.assert_call_counts(
            NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID, expected_number_of_times_processed, 1)

        # TODO: Call if no rules match and no further enrichments?

    @unittest.skip("Flaky test")
    def test_with_rules_but_no_enrichments(self):
        add_data_files(self.rules_source, _RULE_FILE_LOCATIONS)

        cookie_ids = _generate_cookie_ids(TestIntegration._NUMBER_OF_COOKIES)
        cookie_ids.append(NAME_RULE_MATCH_COOKIE)
        expected_number_of_times_processed = len(cookie_ids)
        block_until_processed(self.cookie_jar, cookie_ids, expected_number_of_times_processed)

        self.assertEqual(self.cookie_jar.mark_as_complete.call_count, expected_number_of_times_processed)
        self.cookie_jar.mark_as_failed.assert_not_called()

        rule_checker = RuleChecker(self, self.rules_source.get_all())
        rule_checker.assert_call_counts(
            ALL_MATCH_RULE_ID, expected_number_of_times_processed, expected_number_of_times_processed)
        rule_checker.assert_call_counts(
            NO_MATCH_RULE_ID, expected_number_of_times_processed, 0)
        rule_checker.assert_call_counts(
            NAME_MATCH_RULE_ID, expected_number_of_times_processed, 1)
        rule_checker.assert_call_counts(
            HASH_ENRICHED_MATCH_RULE_ID, expected_number_of_times_processed, 0)

    @unittest.skip("Flaky test")
    def test_with_rules_and_enrichments(self):
        add_data_files(self.rules_source, _RULE_FILE_LOCATIONS)
        assert len(self.rules_source.get_all()) == len(_RULE_FILE_LOCATIONS)
        add_data_files(self.enrichment_loader_source, _ENRICHMENT_LOADER_LOCATIONS)
        assert len(self.enrichment_loader_source.get_all()) == len(_ENRICHMENT_LOADER_LOCATIONS)

        cookie_ids = _generate_cookie_ids(TestIntegration._NUMBER_OF_COOKIES)
        cookie_ids.append(NAME_ENRICHMENT_LOADER_MATCH_COOKIE)
        cookie_ids.append(NAME_RULE_MATCH_COOKIE)
        expected_number_of_times_processed = len(cookie_ids) * 2 + 1
        block_until_processed(self.cookie_jar, cookie_ids, expected_number_of_times_processed)

        self.assertEqual(self.cookie_jar.mark_as_complete.call_count, expected_number_of_times_processed)
        self.cookie_jar.mark_as_failed.assert_not_called()

        rule_checker = RuleChecker(self, self.rules_source.get_all())
        rule_checker.assert_call_counts(
            ALL_MATCH_RULE_ID, expected_number_of_times_processed, expected_number_of_times_processed)
        rule_checker.assert_call_counts(
            NO_MATCH_RULE_ID, expected_number_of_times_processed, 0)
        rule_checker.assert_call_counts(
            NAME_MATCH_RULE_ID, expected_number_of_times_processed, len(_ENRICHMENT_LOADER_LOCATIONS) - 1)
        rule_checker.assert_call_counts(
            HASH_ENRICHED_MATCH_RULE_ID, expected_number_of_times_processed, len(cookie_ids))

        enrichment_loader_checker = EnrichmentLoaderChecker(self, self.enrichment_loader_source.get_all())
        enrichment_loader_checker.assert_call_counts(
            NO_LOADER_ENRICHMENT_LOADER_ID, expected_number_of_times_processed, 0)
        enrichment_loader_checker.assert_call_counts(
            HASH_ENRICHMENT_LOADER_ID, expected_number_of_times_processed - 1, len(cookie_ids))
        enrichment_loader_checker.assert_call_counts(
            NAME_MATCH_LOADER_ENRICHMENT_LOADER_ID, expected_number_of_times_processed, 1)


if __name__ == "__main__":
    unittest.main()
