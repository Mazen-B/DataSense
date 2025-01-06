import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class TestValidateTimeColumn(unittest.TestCase):

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_missing_time_column(self, mock_log_error):
        """ 
      This test checks that an error is raised if the time column is missing 
      """
        df = pd.DataFrame({"date": ["2025-01-01", "2025-01-02"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        
        mock_log_error.side_effect = ValueError("The specified time column 'time' does not exist in the DataFrame.")

        with self.assertRaises(ValueError) as context:
            preprocessor.validate_time_column()
        
        self.assertEqual(
            str(context.exception), 
            "The specified time column 'time' does not exist in the DataFrame.")
        mock_log_error.assert_called_once_with("The specified time column 'time' does not exist in the DataFrame.")

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_invalid_data_type(self, mock_log_error):
        """ 
      This test checks that an error is raised if the time column contains unsupported data types 
      """
        df = pd.DataFrame({"time": [True, False, None]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        mock_log_error.side_effect = ValueError("The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")

        with self.assertRaises(ValueError) as context:
            preprocessor.validate_time_column()

        self.assertEqual(
            str(context.exception),
            "The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")
        mock_log_error.assert_called_once_with("The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_mixed_data_type(self, mock_log_error):
        """ 
      This test checks that an error is raised for a mixed data type in the time column 
      """
        df = pd.DataFrame({"time": ["2025-01-01", 123, pd.NaT, None]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        mock_log_error.side_effect = ValueError("The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")

        with self.assertRaises(ValueError) as context:
            preprocessor.validate_time_column()

        self.assertEqual(
            str(context.exception),
            "The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")
        mock_log_error.assert_called_once_with("The time column 'time' contains unsupported data types. Supported types are string, datetime, or numeric.")

    def test_valid_string_data_type(self):
        """ 
      This test checks validation passes for a valid string data type in the time column 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", None]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        
        # should not raise any error
        preprocessor.validate_time_column()

    def test_valid_datetime_data_type(self):
        """ 
      This test checks that validation passes for a valid datetime data type in the time column 
      """
        df = pd.DataFrame({"time": pd.to_datetime(["2025-01-01", "2025-01-02", None])})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        
        # should not raise any error
        preprocessor.validate_time_column()

    def test_valid_numeric_data_type(self):
        """ 
      This test checks that validation passes for a valid numeric data type in the time column 
      """
        df = pd.DataFrame({"time": [1672531200, 1672617600, None]})  # timestamps
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        
        # should not raise any error
        preprocessor.validate_time_column()

if __name__ == "__main__":
    unittest.main()
