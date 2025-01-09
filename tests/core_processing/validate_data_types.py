import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestValidateDataTypes(unittest.TestCase):

    def setUp(self):
        self.df_valid = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "int_column": [1, 2, 3, 4, 5, 6],
            "float_column": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6]
        })

        self.df_invalid_type = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "non_numeric_column": ["A", "B", "C", "D", "E", "F"]
        })

        self.df_mixed_type = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "mixed_column": ["A", 1, 2, "D", None, "F"]
        })

        self.df_with_empty = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "int_column": [1, 2, None, 4, 5, 6],
            "float_column": [1.1, 2.2, 3.3, None, 5.5, 6.6]
        })

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_validate_data_types_valid(self, mock_log_error):
        """
      This test checks that validate_data_types passes when all columns have valid types.
      """
        checker = DataChecker(self.df_valid.copy(), sensors=["int_column", "float_column"], time_column="time")
        checker.validate_data_types()
        mock_log_error.assert_not_called()

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_validate_data_types_invalid(self, mock_log_error):
        """
      This test checks that validate_data_types raises an error when non-numeric columns are present.
      """
        checker = DataChecker(self.df_invalid_type.copy(), sensors=["non_numeric_column"], time_column="time")
        mock_log_error.side_effect = ValueError("Non-numeric columns detected (excluding time column): ['non_numeric_column']")
        
        with self.assertRaises(ValueError) as context:
            checker.validate_data_types()
        
        self.assertEqual(str(context.exception), "Non-numeric columns detected (excluding time column): ['non_numeric_column']")
        mock_log_error.assert_called_once_with("Non-numeric columns detected (excluding time column): ['non_numeric_column']")

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_validate_mixed_data_types_invalid(self, mock_log_error):
        """
      This test checks that validate_data_types raises an error when mixed data type (including non-numeric) columns are present.
      """
        checker = DataChecker(self.df_mixed_type.copy(), sensors=["mixed_column"], time_column="time")
        mock_log_error.side_effect = ValueError("Non-numeric columns detected (excluding time column): ['mixed_column']")
        
        with self.assertRaises(ValueError) as context:
            checker.validate_data_types()
        
        self.assertEqual(str(context.exception), "Non-numeric columns detected (excluding time column): ['mixed_column']")
        mock_log_error.assert_called_once_with("Non-numeric columns detected (excluding time column): ['mixed_column']")

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_last_emptness_check_no_empty(self, mock_log_error):
        """
      This test checks that last_emptness_check passes when there are no empty values.
      """
        checker = DataChecker(self.df_valid.copy(), sensors=["int_column", "float_column"], time_column="time")
        checker.last_emptness_check()
        mock_log_error.assert_not_called()

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_last_emptness_check_with_empty(self, mock_log_error):
        """
      This test checks that last_emptness_check raises an error when empty values are present.
      """
        checker = DataChecker(self.df_with_empty.copy(), sensors=["int_column", "float_column"], time_column="time")
        mock_log_error.side_effect = ValueError("Data validation failed: 2 empty values remain. Details: {'int_column': [Timestamp('2025-01-03 00:00:00')], 'float_column': [Timestamp('2025-01-04 00:00:00')]}")

        with self.assertRaises(ValueError) as context:
            checker.last_emptness_check()

        self.assertEqual(
            str(context.exception),
            "Data validation failed: 2 empty values remain. Details: {'int_column': [Timestamp('2025-01-03 00:00:00')], 'float_column': [Timestamp('2025-01-04 00:00:00')]}")

        mock_log_error.assert_called_once_with("Data validation failed: 2 empty values remain. Details: {'int_column': [Timestamp('2025-01-03 00:00:00')], 'float_column': [Timestamp('2025-01-04 00:00:00')]}")

if __name__ == "__main__":
    unittest.main()
