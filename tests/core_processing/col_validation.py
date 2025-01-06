import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestColumnValidation(unittest.TestCase):

    def test_standardize_column_names(self):
        """
      This test checks that column names are standardized correctly (e.g., lowercase, leading/trailing spaces, etc.).
      """
        df = pd.DataFrame({" Column 1 ": [1, 2], "COLUMN 2": [3, 4], "col 3 ": [5, 6]})
        checker = DataChecker(df)

        checker.standardize_column_names()

        expected_columns = ["column_1", "column_2", "col_3"]
        self.assertListEqual(checker.df.columns.tolist(), expected_columns)


    def test_validate_columns_success(self):
        """
      This test checks that validate_columns passes when all required columns are present.
      """
        df = pd.DataFrame({"sensor1": [1, 2, 3], "sensor2": [4, 5, 6], "time": [7, 8, 9]})
        checker = DataChecker(df, sensors=["sensor1", "sensor2"], time_column="time")

        checker.validate_columns()

        # no exception should be raised, meaning validation passed
        self.assertTrue(True)

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_validate_columns_missing_columns(self, mock_log_error):
        """
      This test checks that validate_columns raises an error if required columns are missing.
      """
        df = pd.DataFrame({"sensor1": [1, 2, 3]})
        checker = DataChecker(df, sensors=["sensor1", "sensor2"], time_column="time")

        mock_log_error.side_effect = ValueError("Missing columns in data: ['sensor2', 'time']")

        with self.assertRaises(ValueError) as context:
            checker.validate_columns()
        
        self.assertEqual(str(context.exception), "Missing columns in data: ['sensor2', 'time']")
        mock_log_error.assert_called_once_with("Missing columns in data: ['sensor2', 'time']")

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_validate_columns_empty_columns(self, mock_log_error):
        """
      This test checks that validate_columns raises an error if any required columns are completely empty.
      """
        df = pd.DataFrame({"sensor1": [1, 2, 3], "sensor2": [None, None, None], "time": [7, 8, 9]})
        checker = DataChecker(df, sensors=["sensor1", "sensor2"], time_column="time")

        mock_log_error.side_effect = ValueError("The following columns are completely empty: ['sensor2']")

        with self.assertRaises(ValueError) as context:
            checker.validate_columns()
        
        self.assertEqual(str(context.exception), "The following columns are completely empty: ['sensor2']")
        mock_log_error.assert_called_once_with("The following columns are completely empty: ['sensor2']")

if __name__ == "__main__":
    unittest.main()
