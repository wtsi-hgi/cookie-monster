import unittest

from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.tests.cookiejar.test_cookiejar import TestCookieJar, HiddenTestCookieJar


class TestInMemoryCookieJar(HiddenTestCookieJar[0]):
    """
    Tests for `InMemoryCookieJar`.
    """
    def _create_cookie_jar(self) -> CookieJar:
        return InMemoryCookieJar()

    def _change_time(self, change_time_to: int):
        pass


if __name__ == "__main__":
    unittest.main()
