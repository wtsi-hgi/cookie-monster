import unittest
from datetime import date

from cookiemonster.common.models import Model


class _StubModel(Model):
    """
    Stub `Model`.
    """
    def __init__(self):
        self.property_1 = 1
        self.property_2 = "a"
        self.property_3 = []


class TestModel(unittest.TestCase):
    """
    Test cases for `Model`.
    """
    def setUp(self):
        self._model = _StubModel()

    def test_equal_non_nullity(self):
        self.assertNotEqual(self._model, None)

    def test_equal_different_type(self):
        self.assertNotEqual(self._model, date)

    def test_equal_reflexivity(self):
        model = self._model
        self.assertEqual(model, model)

    def test_equal_symmetry(self):
        model1 = self._model
        model2 = self._model
        self.assertEqual(model1, model2)
        self.assertEqual(model2, model1)

    def test_equal_transitivity(self):
        model1 = self._model
        model2 = self._model
        model3 = self._model
        self.assertEqual(model1, model2)
        self.assertEqual(model2, model3)
        self.assertEqual(model1, model3)

    def test_can_get_string_representation(self):
        string_representation = str(self._model)
        self.assertTrue(isinstance(string_representation, str))
