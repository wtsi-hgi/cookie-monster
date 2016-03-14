# GPLv3 or later
# Copyright (c) 2016 Genome Research Limited
import unittest
from abc import ABCMeta, abstractmethod

from cookiemonster.common.decorators import too_big_to_fail, MaxAttemptsExhausted


class ABCStub(metaclass=ABCMeta):
    @abstractmethod
    def this_will_fail_unless(self, condition:bool) -> bool:
        ''' This method will raise an exception until condition is True '''

@too_big_to_fail
class Stub(ABCStub):
    def this_will_fail_unless(self, condition:bool) -> bool:
        if not condition:
            raise Exception

        return True


class TestCatcherDecorator(unittest.TestCase):
    def test_fail_after_max_attempts(self):
        tries = 10
        obj = Stub(max_attempts=tries)

        for x in range(1, tries + 1):
            if x == tries:
                self.assertEqual(obj.this_will_fail_unless(x == tries), True)
            else:
                with self.assertRaises(MaxAttemptsExhausted):
                    obj.this_will_fail_unless(x == tries)

if __name__ == '__main__':
    unittest.main()
