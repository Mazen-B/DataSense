import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class TestHandleMissingValues(unittest.TestCase):

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_missing_values_error(self, mock_log_error):
        """ 
      This test checks that an error is raised when missing values are present and method is set to 'error' 
      """
        df = pd.DataFrame({"time": ["2025-01-01", None, "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        
        mock_log_error.side_effect = ValueError("The time column 'time' contains 1 missing (NaN) values. Please clean the data or set 'handle_missing' to 'drop'.")

        with self.assertRaises(ValueError) as context:
            preprocessor.handle_missing_values(method="error")
        
        self.assertEqual(
            str(context.exception),
            "The time column 'time' contains 1 missing (NaN) values. Please clean the data or set 'handle_missing' to 'drop'.")
        mock_log_error.assert_called_once_with("The time column 'time' contains 1 missing (NaN) values. Please clean the data or set 'handle_missing' to 'drop'.")

    @patch("data_manager.preprocessing.time_preprocessor.logging.warning")
    def test_missing_values_drop(self, mock_log_warning):
        """ 
      This test checks that rows with missing values are dropped when method is set to 'drop' 
      """
        df = pd.DataFrame({"time": ["2025-01-01", None, "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.handle_missing_values(method="drop")
        
        # check if the missing row was removed
        self.assertEqual(len(preprocessor.df), 2)
        self.assertListEqual(preprocessor.df["time"].tolist(), ["2025-01-01", "2025-01-03"])
        self.assertNotIn(None, preprocessor.df["time"].values)

        # check if the correct warning was logged
        mock_log_warning.assert_called_once_with("Dropping 1 rows with missing values in the 'time' column.")

    def test_no_missing_values(self):
        """ 
      This test checks that nothing happens when there are no missing values 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.handle_missing_values(method="error")

        # check if DataFrame remains unchanged
        self.assertEqual(len(preprocessor.df), 3)
        self.assertListEqual(preprocessor.df["time"].tolist(), ["2025-01-01", "2025-01-02", "2025-01-03"])

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_invalid_method(self, mock_log_error):
        """ 
      This test checks that an error is raised when an invalid method is passed 
      """
        df = pd.DataFrame({"time": ["2025-01-01", None, "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        mock_log_error.side_effect = ValueError("Invalid method: 'invalid_method' for handling missing values in time column preprocessing, allowed method: error or drop.")

        with self.assertRaises(ValueError) as context:
            preprocessor.handle_missing_values(method="invalid_method")

        self.assertEqual(
            str(context.exception),
            "Invalid method: 'invalid_method' for handling missing values in time column preprocessing, allowed method: error or drop.")

        # ensure no changes are made to the DataFrame
        self.assertEqual(len(preprocessor.df), 3)
        self.assertTrue(preprocessor.df["time"].isna().any())

        mock_log_error.assert_called_once_with("Invalid method: 'invalid_method' for handling missing values in time column preprocessing, allowed method: error or drop.")

if __name__ == "__main__":
    unittest.main()
