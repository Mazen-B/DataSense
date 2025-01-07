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
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, None, 4, 5, None],
            "sensor2": [None, 2, 4, None, 8, 2]
        })

    def test_handle_missing_values_fill_ffill(self):
        """
      This test checks that missing values are forward filled when fill method is "ffill".
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="ffill")
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 2, 4, 5, 5],
            "sensor2": [2, 2, 4, 4, 8, 2] # first one should be filled using bfill
        })

        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    def test_handle_missing_values_fill_bfill(self):
        """
      This test checks that missing values are backward filled when fill method is "bfill".
      Here we are also checking if the last value will be replaced using the "ffill" method instead of an empty one.
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="bfill")
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 4, 4, 5, 5], # last one should use ffill
            "sensor2": [2, 2, 4, 8, 8, 2] 
        })
        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    def test_handle_missing_values_fill_mean(self):
        """
      This test checks that missing values are filled with mean of the whole column when fill method is "mean".
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mean")
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 3, 4, 5, 3], # mean is 3
            "sensor2": [4, 2, 4, 4, 8, 2]  # mean is 4
        })

        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    def test_handle_missing_values_fill_constant(self):
        """
      This test checks that missing values are filled with one specified value, when fill method is "constant".
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="constant", fill_value=0)
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 0, 4, 5, 0],
            "sensor2": [0, 2, 4, 0, 8, 2]
        })

        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    def test_handle_missing_values_fill_mode(self):
        """
      This test checks that missing values are filled with the most common value of the column, when fill method is "mode".
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mode")
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 1, 4, 5, 1],  # mode is 1
            "sensor2": [2, 2, 4, 2, 8, 2]   # mode is 2
        })

        expected_df["time"] = pd.to_datetime(expected_df["time"])
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)

        pd.testing.assert_frame_equal(checker.df, expected_df)

    def test_handle_missing_values_fill_interpolate(self):
        """
      This test checks that missing values are filled using linear interpolation.
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="interpolate")
        
        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="D"),
            "sensor1": [1, 2, 3, 4, 5, 5],  # linearly interpolated, but for the last we use ffill
            "sensor2": [2, 2, 4, 6, 8, 2] # linearly interpolated, but for the one we use bfill
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

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_handle_missing_values_invalid_fill_method(self, mock_log_error):
        """
      This test checks that an error is raised when an unknown fill method is provided.
      """
        checker = DataChecker(self.df.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        mock_log_error.side_effect = ValueError("Invalid fill method 'unknown' provided.")
        
        with self.assertRaises(ValueError) as context:
            checker.handle_missing_values(strategy="fill", fill_method="unknown")
        
        self.assertEqual(str(context.exception), "Invalid fill method 'unknown' provided.")
        mock_log_error.assert_called_once_with("Invalid fill method 'unknown' provided.")

if __name__ == "__main__":
    unittest.main()
