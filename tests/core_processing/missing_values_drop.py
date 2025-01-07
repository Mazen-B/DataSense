import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestHandleMissingValues(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=5, freq="D"),
            "sensor1": [1, None, 3, 4, 5],
            "sensor2": [2, 4, None, 8, None]
        })

    def test_handle_missing_values_drop(self):
        """
      This test checks that rows with missing values are correctly dropped when strategy is "drop".
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="drop", fill_method=None)
        
        # only rows without any missing values should remain
        expected_df = pd.DataFrame({
            "time": ["2025-01-01","2025-01-04"],
            "sensor1": [1, 4],
            "sensor2": [2, 8]
        })
        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_handle_missing_values_invalid_strategy(self, mock_log_error):
        """
      This test checks that an error is raised when an unknown strategy is provided.
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")

        mock_log_error.side_effect = ValueError("Unknown strategy 'invalid' provided for handling missing values.")
        
        with self.assertRaises(ValueError) as context:
            checker.handle_missing_values(strategy="invalid", fill_method=None)
        
        self.assertEqual(str(context.exception), "Unknown strategy 'invalid' provided for handling missing values.")
        mock_log_error.assert_called_once_with("Unknown strategy 'invalid' provided for handling missing values.")

if __name__ == "__main__":
    unittest.main()
