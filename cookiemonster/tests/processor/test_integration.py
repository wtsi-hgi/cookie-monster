import shutil
import unittest
from copy import copy
from datetime import datetime
from os.path import normpath, join, dirname, realpath
from tempfile import mkdtemp
from threading import Semaphore
from typing import Sequence, Tuple, Optional, Iterable
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment, Notification
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._enrichment import EnrichmentManager, EnrichmentLoaderSource
from cookiemonster.processor._rules import RuleSource
from cookiemonster.processor.basic_processing import BasicProcessorManager
from cookiemonster.processor.processing import ProcessorManager
from cookiemonster.tests.processor._helpers import add_data_files, block_until_processed
from cookiemonster.tests.processor._mocks import create_magic_mock_cookie_jar
from cookiemonster.tests.processor._stubs import StubNotifier
from cookiemonster.tests.processor.example_rule.enrich_match_rule import MATCHES_ENIRCHED_COOKIE_WITH_PATH
from cookiemonster.tests.processor.example_rule.name_match_rule import MATCHES_COOKIES_WITH_PATH, NOTIFIES

_RULE_FILE_LOCATIONS = [
    normpath(join(dirname(realpath(__file__)), "example_rule/no_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "example_rule/name_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "example_rule/enrich_match_rule.py"))
]
_ENRICHMENT_LOADER_LOCATIONS = [
    normpath(join(dirname(realpath(__file__)), "example_enrichment_loader/no_loader.py")),
    normpath(join(dirname(realpath(__file__)), "example_enrichment_loader/hash_loader.py"))
]


class TestIntegration(unittest.TestCase):
    """
    Integration tests for processor.
    """
    _NUMBER_OF_COOKIE_ENRICHMENTS = 1000
    _NUMBER_OF_PROCESSORS = 10
    _PATH = "/my/cookie"

    def setUp(self):
        self.rules_directory = mkdtemp(prefix="rules", suffix=TestIntegration.__name__)
        self.enrichment_loaders_directory = mkdtemp(prefix="enrichment_loaders", suffix=TestIntegration.__name__)

        # Setup enrichment
        self.enrichment_loader_source = EnrichmentLoaderSource(self.enrichment_loaders_directory)
        self.enrichment_loader_source.start()
        enrichment_manager = EnrichmentManager(self.enrichment_loader_source)

        # Setup cookie jar
        self.cookie_jar = create_magic_mock_cookie_jar()

        # Setup rules source
        self.rules_source = RuleSource(self.rules_directory)
        self.rules_source.start()

        # Setup notifier
        self.notifier = StubNotifier()   # type: Notifier
        self.notifier.do = MagicMock()

        # Setup the data processor manager
        self.processor_manager = BasicProcessorManager(
                TestIntegration._NUMBER_OF_PROCESSORS, self.cookie_jar, self.rules_source, enrichment_manager,
                self.notifier)   # type: ProcessorManager

        def cookie_jar_connector(*args):
            self.processor_manager.process_any_cookies()

        self.cookie_jar.add_listener(cookie_jar_connector)

    def test_with_no_rules_or_enrichments(self):
        cookie_paths = TestIntegration._generate_cookie_paths(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS)
        block_until_processed(self.cookie_jar, cookie_paths)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_paths))
        self.assertEquals(self.notifier.do.call_count, len(cookie_paths))
        self.cookie_jar.mark_as_failed.assert_not_called()

    def test_with_enrichments_no_rules(self):
        add_data_files(self.enrichment_loader_source, _ENRICHMENT_LOADER_LOCATIONS)

        cookie_paths = TestIntegration._generate_cookie_paths(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS)
        block_until_processed(self.cookie_jar, cookie_paths)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_paths))
        self.assertEquals(self.notifier.do.call_count, len(cookie_paths))
        self.cookie_jar.mark_as_failed.assert_not_called()

    def test_with_rules_no_enrichments(self):
        add_data_files(self.rules_source, _RULE_FILE_LOCATIONS)

        cookie_paths = list(TestIntegration._generate_cookie_paths(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS))
        cookie_paths.append(MATCHES_COOKIES_WITH_PATH)
        block_until_processed(self.cookie_jar, cookie_paths)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_paths))
        self.assertEquals(self.notifier.do.call_count, len(cookie_paths))
        self.cookie_jar.mark_as_failed.assert_not_called()
        self.assertIn(call(Notification(NOTIFIES, MATCHES_COOKIES_WITH_PATH)), self.notifier.do.call_args_list)

    def test_with_rules_and_enrichments(self):
        add_data_files(self.rules_source, _RULE_FILE_LOCATIONS)
        add_data_files(self.enrichment_loader_source, _ENRICHMENT_LOADER_LOCATIONS)

        cookie_paths = list(TestIntegration._generate_cookie_paths(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS))
        cookie_paths.append(MATCHES_ENIRCHED_COOKIE_WITH_PATH)
        block_until_processed(self.cookie_jar, cookie_paths)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_paths))
        self.assertEquals(self.notifier.do.call_count, len(cookie_paths))
        self.cookie_jar.mark_as_failed.assert_not_called()
        self.assertIn(call(Notification(NOTIFIES, MATCHES_COOKIES_WITH_PATH)), self.notifier.do.call_args_list)

    def tearDown(self):
        shutil.rmtree(self.rules_directory)
        shutil.rmtree(self.enrichment_loaders_directory)

    @staticmethod
    def _generate_cookie_paths(number: int) -> Sequence[str]:
        """
        Generates the given number of example cookie paths.
        :param number: the number of example cookie paths to generate
        :return: the generated cookie paths
        """
        return ["%s/%s" % (TestIntegration._PATH, i) for i in range(number)]
