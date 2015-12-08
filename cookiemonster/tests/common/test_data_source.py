import unittest

from cookiemonster.common.data_source import MultiDataSource, StaticDataSource
from cookiemonster.common.models import Notification
from cookiemonster.processor._models import Rule
from cookiemonster.processor._models import RuleAction


class TestMultiSource(unittest.TestCase):
    """
    Tests for `MultiDataSource`.
    """
    def setUp(self):
        self.data = [i for i in range(10)]
        self.sources = [StaticDataSource([self.data[i]]) for i in range(len(self.data))]

    def test_get_when_no_sources(self):
        source = MultiDataSource()
        self.assertEquals(len(source.get_all()), 0)

    def test_get_when_source(self):
        source = MultiDataSource(self.sources)
        self.assertIsInstance(source.get_all()[0], type(self.data[0]))
        self.assertCountEqual(source.get_all(), self.data)

    def test_init_change_of_source_list_has_no_effect(self):
        source = MultiDataSource(self.sources)
        self.sources.pop()
        self.assertCountEqual(source.get_all(), self.data)


if __name__ == "__main__":
    unittest.main()
