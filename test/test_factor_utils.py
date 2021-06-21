import unittest
import pandas as pd

from dqc.rule.utils import factor_utils
# from dqc.rule.utils.factor_utils import __check_minute


class MyTestCase(unittest.TestCase):
    def test_factor_type(self):

        self.assertEqual(True,factor_utils.check_value_match_type(pd.DataFrame([0,2,9])[0], "unsigned"))
        self.assertEqual(False, factor_utils.check_value_match_type(pd.DataFrame([0, 2, -2])[0], "unsigned"))
        self.assertEqual(True, factor_utils.check_value_match_type(pd.DataFrame([0,2,39])[0], "minute"))
        self.assertEqual(True, factor_utils.check_value_match_type(pd.DataFrame([1, 2])[0], "half-year"))


if __name__ == '__main__':
    unittest.main()
