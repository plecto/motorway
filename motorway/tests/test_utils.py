from unittest import TestCase
import unittest
from motorway.utils import percentile_from_dict


class UtilsTestCase(TestCase):
    def test_percentile(self):
        test_dict = {0.0: 2, 1.0: 1, 2.0: 1, 3.0: 3, 4.0: 1}
        self.assertEqual(percentile_from_dict(test_dict, 95), 4.0)
