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
from datetime import timedelta
from typing import Optional

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar import CookieJar
from cookiemonster.processor.processing import Processor


class StubCookieJar(CookieJar):
    """
    Stub implementation of `CookieJar`.
    """
    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        pass

    def mark_as_complete(self, path: str):
        pass

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        pass

    def mark_for_processing(self, path: str):
        pass

    def queue_length(self) -> int:
        pass

    def get_next_for_processing(self) -> Optional[Cookie]:
        pass


class StubProcessor(Processor):
    """
    Stub implementation of `Processor`.
    """
    def handle_cookie_enrichment(self, cookie: Cookie):
        pass

    def evaluate_rules_with_cookie(self, cookie: Cookie) -> bool:
        pass
