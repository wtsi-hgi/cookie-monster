# GPLv3 or later
# Copyright (c) 2016 Genome Research Limited
import unittest
from abc import ABCMeta, abstractmethod

from cookiemonster.common.decorators import too_big_to_fail, MaxAttemptsExhausted


class TestException(Exception):
    pass


class ABCStub(metaclass=ABCMeta):
    @abstractmethod
    def this_will_fail_unless(self, condition:bool) -> bool:
        """ This method will raise an exception unless condition is True """

@too_big_to_fail()
class CatchEverything(ABCStub):
    def this_will_fail_unless(self, condition:bool) -> bool:
        if not condition:
            raise Exception

        return True

@too_big_to_fail(TestException)
class CatchSpecificExceptions(ABCStub):
    def this_will_fail_unless(self, condition: bool) -> bool:
        raise TestException if condition else Exception


# Turing must be spinning in his grave :P
class TestCatchEverything(unittest.TestCase):
    def test_fail_after_max_attempts(self):
        tries = 10
        obj = CatchEverything(max_attempts=tries)

        for x in range(1, tries + 1):
            if x == tries:
                self.assertTrue(obj.this_will_fail_unless(x == tries))
            else:
                with self.assertRaises(MaxAttemptsExhausted):
                    obj.this_will_fail_unless(x == tries)

class TestCatchSpecific(unittest.TestCase):
    def test_suppress_specific(self):
        obj = CatchSpecificExceptions(max_attempts=1)

        with self.assertRaises(Exception):
            obj.this_will_fail_unless(False)

        with self.assertRaises(MaxAttemptsExhausted):
            obj.this_will_fail_unless(True)


if __name__ == '__main__':
    unittest.main()
