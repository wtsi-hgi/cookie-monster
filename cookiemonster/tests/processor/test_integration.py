from copy import copy
import logging
import shutil
import unittest
from datetime import datetime
from multiprocessing import Lock
from os.path import normpath, join, dirname, realpath
from tempfile import mkdtemp
from threading import Semaphore
from typing import Sequence, Tuple, List, Iterable
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata
from hgicommon.data_source import RegisteringDataSource, SynchronisedFilesDataSource
from hgicommon.data_source.static_from_file import FileSystemChange

from cookiemonster.common.models import Enrichment, Notification
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._enrichment import EnrichmentManager, EnrichmentLoaderSource
from cookiemonster.processor._rules import RuleSource
from cookiemonster.processor.basic_processing import BasicProcessorManager
from cookiemonster.processor.processing import ProcessorManager
from cookiemonster.tests.processor._helpers import add_data_files
from cookiemonster.tests.processor._stubs import StubNotifier
from cookiemonster.tests.processor.example_rule.name_match_rule import MATCHES_COOKIES_WITH_PATH, NOTIFIES

_RULE_FILE_LOCATIONS = [
    normpath(join(dirname(realpath(__file__)), "example_rule/no_match_rule.py")),
    normpath(join(dirname(realpath(__file__)), "example_rule/name_match_rule.py"))
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
        self.cookie_jar = InMemoryCookieJar()    # type: CookieJar

        # Setup rules source
        self.rules_source = RuleSource(self.rules_directory)
        self.rules_source.start()

        # Setup notifier
        self.notifier = StubNotifier()   # type: Notifier

        # Setup the data processor manager
        self.processor_manager = BasicProcessorManager(
                TestIntegration._NUMBER_OF_PROCESSORS, self.cookie_jar, self.rules_source, enrichment_manager,
                self.notifier)   # type: ProcessorManager

        def cookie_jar_connector(*args):
            self.processor_manager.process_any_cookies()

        self.cookie_jar.add_listener(cookie_jar_connector)

        # Enable better debugging
        # logging.root.setLevel(logging.DEBUG)

    def test_with_no_rules_or_enrichments(self):
        cookie_enrichments = TestIntegration._generate_n_cookie_enrichments(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS)
        self._process_cookies(cookie_enrichments)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_enrichments))
        self.assertEquals(self.notifier.do.call_count, len(cookie_enrichments))
        self.cookie_jar.mark_as_failed.assert_not_called()
        self.cookie_jar.mark_for_processing.assert_not_called()

    def test_with_rules_enrichments(self):
        add_data_files(self.enrichment_loader_source, _ENRICHMENT_LOADER_LOCATIONS)

        cookie_enrichments = TestIntegration._generate_n_cookie_enrichments(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS)
        self._process_cookies(cookie_enrichments)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_enrichments))
        self.assertEquals(self.notifier.do.call_count, len(cookie_enrichments))
        self.cookie_jar.mark_as_failed.assert_not_called()
        self.cookie_jar.mark_for_processing.assert_not_called()

    def test_with_rules_no_enrichments(self):
        add_data_files(self.rules_source, _RULE_FILE_LOCATIONS)

        cookie_enrichments = list(
                TestIntegration._generate_n_cookie_enrichments(TestIntegration._NUMBER_OF_COOKIE_ENRICHMENTS))
        cookie_enrichments.append((MATCHES_COOKIES_WITH_PATH, Enrichment("source", datetime.max, Metadata())))

        self._process_cookies(cookie_enrichments)

        self.assertEquals(self.cookie_jar.mark_as_complete.call_count, len(cookie_enrichments))
        self.assertEquals(self.notifier.do.call_count, len(cookie_enrichments))
        self.cookie_jar.mark_as_failed.assert_not_called()
        self.cookie_jar.mark_for_processing.assert_not_called()
        self.assertIn(call(Notification(NOTIFIES, MATCHES_COOKIES_WITH_PATH)), self.notifier.do.call_args_list)

    def _process_cookies(self, cookie_enrichments: Sequence[Tuple[str, Enrichment]]):
        """
        Processes the given cookie enrichments, blocking until all processing has been completed.
        :param cookie_enrichments: the cookie enrichments to process where the first item in the tuple is the cookie's
        path and the second is the enrichment
        """
        processed_semaphore = Semaphore(0)

        original_mark_as_completed = self.cookie_jar.mark_as_complete
        original_mark_for_processing = self.cookie_jar.mark_for_processing

        def on_complete(path: str):
            processed_semaphore.release()
            original_mark_as_completed(path)

        def on_reprocess(path: str):
            processed_semaphore.release()
            original_mark_for_processing(path)

        self.notifier.do = MagicMock()
        self.cookie_jar.mark_as_failed = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)
        self.cookie_jar.mark_for_processing = MagicMock(side_effect=on_reprocess)

        for cookie_enrichment in cookie_enrichments:
            self.cookie_jar.enrich_cookie(cookie_enrichment[0], cookie_enrichment[1])

        processed = 0
        while processed != len(cookie_enrichments):
            processed_semaphore.acquire()
            processed += 1

    def tearDown(self):
        shutil.rmtree(self.rules_directory)
        shutil.rmtree(self.enrichment_loaders_directory)

    @staticmethod
    def _generate_n_cookie_enrichments(number: int) -> Sequence[Tuple[str, Enrichment]]:
        """
        Generates the given number of example cookie enrichments.
        :param number: the number of example cookie enrichments to generate
        :return: the generated cookie enrichments
        """
        enrichment = Enrichment("my_source", datetime.min, Metadata())
        cookie_enrichments = [("%s/%s" % (TestIntegration._PATH, i), copy(enrichment)) for i in range(number)]
        return cookie_enrichments
