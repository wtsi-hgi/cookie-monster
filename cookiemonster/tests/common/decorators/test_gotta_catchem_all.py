"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

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
