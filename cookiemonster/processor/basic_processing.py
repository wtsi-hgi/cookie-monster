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
import copy
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Sequence

from hgicommon.data_source import DataSource

from cookiemonster.common.models import Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.logging.logger import PythonLoggingLogger, Logger
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._rules import RuleQueue
from cookiemonster.processor.models import Rule, EnrichmentLoader
from cookiemonster.processor.processing import ProcessorManager, Processor

_MEASUREMENT_PROCESSING_COUNT = "processing"
_MEASUREMENT_GET_NEXT_COUNT = "get_next_for_processing"
_MEASUREMENT_TIME_TO_PROCESS = "time_to_process"


class BasicProcessor(Processor):
    """
    Simple processor for a single Cookie.
    """
    def __init__(self, cookie_jar: CookieJar, rules: Sequence[Rule], enrichment_loaders: Sequence[EnrichmentLoader]):
        """
        Constructor.
        :param cookie_jar: the cookie jar to use
        :param rules: the rules to process the Cookie with
        :param enrichment_loaders: the enrichment loaders that may be able to enrich the Cookie
        """
        self.cookie_jar = cookie_jar
        self.rules = rules
        self.enrichment_loaders = enrichment_loaders

    def evaluate_rules_with_cookie(self, cookie: Cookie) -> bool:
        rule_queue = RuleQueue(self.rules)
        terminate = False

        while not terminate and rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            isolated_cookie = copy.deepcopy(cookie)
            try:
                if rule.matches(isolated_cookie):
                    terminate = rule.execute_action(isolated_cookie)
            except Exception:
                logging.error("Error applying rule; Rule: %s; Error: %s" % (rule, traceback.format_exc()))
            rule_queue.mark_as_applied(rule)

        return terminate

    def handle_cookie_enrichment(self, cookie: Cookie):
        logging.info("Checking if any of the %d enrichment loader(s) can load enrichment for cookie with identifier "
                     "\"%s\"" % (len(self.enrichment_loaders), cookie.identifier))
        enrichment_manager = EnrichmentManager(self.enrichment_loaders)
        enrichment = enrichment_manager.next_enrichment(cookie)

        if enrichment is None:
            logging.info("Cannot enrich cookie with identifier \"%s\" any further - notifying listeners"
                         % cookie.identifier)
            # TODO: Should anything else be done here?
        else:
            logging.info("Applying enrichment from source \"%s\" to cookie with identifier \"%s\""
                         % (enrichment.source, cookie.identifier))
            self.cookie_jar.enrich_cookie(cookie.identifier, enrichment)
            # Enrichment method sets cookie for processing when enriched so no need to repeat that


class BasicProcessorManager(ProcessorManager):
    """
    Simple manager for the continuous processing of enriched Cookies.
    """
    def __init__(self, cookie_jar: CookieJar, rules_source: DataSource[Rule],
                 enrichment_loaders_source: DataSource[EnrichmentLoader], number_of_threads: int=16,
                 logger: Logger=PythonLoggingLogger()):
        """
        Constructor.
        :param cookie_jar: the cookie jar to get updates from
        :param rules_source: the source of the rules
        :param enrichment_loaders_source: the source of enrichment loaders
        :param number_of_threads: the maximum number of threads to use
        :param logger: log recorder
        """
        if number_of_threads < 1:
            raise ValueError("Must specific the use of at least one thread, not %d" % number_of_threads)

        self._cookie_jar = cookie_jar
        self._rules_source = rules_source
        self._enrichment_loaders_source = enrichment_loaders_source
        self._cookie_processing_thread_pool = ThreadPoolExecutor(max_workers=number_of_threads)
        self._processing_count = 0
        self._get_next_count = 0
        self._logger = logger

    def process_any_cookies(self):
        logging.debug("Prompted to process any unprocessed cookies.")
        self._cookie_processing_thread_pool.submit(self._process_any_cookies)

    def _process_any_cookies(self):
        """
        Processes any cookies, blocking whilst the Cookie is processed.
        """
        # Claim cookie
        self._get_next_count += 1
        self._logger.record(_MEASUREMENT_GET_NEXT_COUNT, self._get_next_count)
        cookie = self._cookie_jar.get_next_for_processing()
        self._get_next_count -= 1
        self._logger.record(_MEASUREMENT_GET_NEXT_COUNT, self._get_next_count)

        if cookie is None:
            logging.info("Triggered to process cookies but none need processing.")
        else:
            # Check if there is more Cookies that need to be processed
            self.process_any_cookies()

            logging.info("Processing cookie with identifier: \"%s\"." % cookie.identifier)
            started_at = time.monotonic()

            processor = BasicProcessor(self._cookie_jar, self._rules_source.get_all(),
                                       self._enrichment_loaders_source.get_all())
            try:
                # Process Cookie
                self._processing_count += 1
                self._logger.record(_MEASUREMENT_PROCESSING_COUNT, self._processing_count)
                processor.process_cookie(cookie)

                # Relinquish claim on Cookie
                self._cookie_jar.mark_as_complete(cookie.identifier)

                total_time = time.monotonic() - started_at
                self._logger.record(_MEASUREMENT_TIME_TO_PROCESS, total_time)
                logging.info("Processed and marked as complete cookie with path \"%s\" in %f seconds (wall time)."
                             % (cookie.identifier, total_time))
            except Exception:
                logging.error("Exception raised whilst processing cookie with identifier \"%s\": %s"
                              % (cookie.identifier, traceback.format_exc()))

                # Relinquish claim on Cookie but state processing as failed
                self._cookie_jar.mark_as_failed(cookie.identifier)
            finally:
                self._processing_count -= 1
                self._logger.record(_MEASUREMENT_PROCESSING_COUNT, self._processing_count)
