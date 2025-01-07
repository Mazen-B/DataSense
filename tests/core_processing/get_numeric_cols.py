import os
import sys
import unittest
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestGetNumericColumns(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=5, freq="D"),
            "sensor1": [1.1, 2.2, 3.3, 4.4, 5.5],
            "sensor2": [10, 20, 30, 40, 50],
            "category": ["A", "B", "C", "D", "E"]
        })

    def test_get_numeric_columns(self):
        """
      This test checks that _get_numeric_columns correctly identifies numeric columns, excluding the time column.
      """
        checker = DataChecker(self.df, sensors=["sensor1", "sensor2"], time_column="time")
        numeric_columns = checker._get_numeric_columns()

        expected_columns = ["sensor1", "sensor2"]

        self.assertListEqual(list(numeric_columns), expected_columns)

    def test_get_numeric_columns_no_time_column(self):
        """
      This test checks the behavior when time_column is not specified, the method should still works correctly.
      """
        checker = DataChecker(self.df, sensors=["sensor1", "sensor2"])
        numeric_columns = checker._get_numeric_columns()

        expected_columns = ["sensor1", "sensor2"]

        self.assertListEqual(list(numeric_columns), expected_columns)

    def test_get_numeric_columns_no_numeric(self):
        """
      This test checks the behavior when there are no numeric columns in the DataFrame.
      """
        df_no_numeric = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=5, freq="D"),
            "category": ["A", "B", "C", "D", "E"]
        })
        checker = DataChecker(df_no_numeric, time_column="time")
        numeric_columns = checker._get_numeric_columns()

        expected_columns = []

        self.assertListEqual(list(numeric_columns), expected_columns)

if __name__ == "__main__":
    unittest.main()
