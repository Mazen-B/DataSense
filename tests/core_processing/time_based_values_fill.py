import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestApplyTimeBasedFill(unittest.TestCase):

    def setUp(self):
        # df with daily frequency
        self.df_days = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="d"),
            "sensor1": [1, 2, None, 4, 5, None],
            "sensor2": [None, 2, 4, None, 8, 2]
        })

        # df with second-level frequency
        self.df_seconds = pd.DataFrame({
            "time": pd.date_range("2025-01-01 00:00:00", periods=6, freq="s"),
            "sensor1": [1, None, 3, None, 5, 6],
            "sensor2": [None, 2, None, 4, None, 2]
        })

        # df with sequence of missing values
        self.df_sequence = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=9, freq="d"),
            "sensor1": [1, None, 3, None, 5, 6, None, None, None],
            "sensor2": [None, 2, None, 4, None, 2, 2, None, None]
        })

        # df with all missing values
        self.df_all_missing = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=5, freq="d"),
            "sensor1": [1, None, None, None, None],
            "sensor2": [None, None, None, None, 2]
        })

        # df with intermittent missing values
        self.df_intermittent = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=10, freq="d"),
            "sensor1": [1, None, None, 4, None, 6, None, 8, None, 10],
            "sensor2": [None, 2, 3, None, 5, None, 7, None, 9, None]
        })

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_time_based_fill_mean_days(self, mock_log_info):
        """
      This test checks that the centered rolling function fills all the missing values using mean.
      """
        checker = DataChecker(self.df_days.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mean", time_window="2d")

        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=6, freq="d"),
            "sensor1": [1, 2, 3, 4, 5, 5],
            "sensor2": [2, 2, 4, 6, 8, 2]
        })
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)
        pd.testing.assert_frame_equal(checker.df, expected_df)

        expected_logs = [
            "At timestamp '2025-01-03 00:00:00', filled using mean: 3.0 (window: 2025-01-02 00:00:00 to 2025-01-04 00:00:00).",
            "At timestamp '2025-01-06 00:00:00', filled using mean: 5.0 (window: 2025-01-05 00:00:00 to 2025-01-06 00:00:00).",
            "At timestamp '2025-01-01 00:00:00', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-02 00:00:00).",
            "At timestamp '2025-01-04 00:00:00', filled using mean: 6.0 (window: 2025-01-03 00:00:00 to 2025-01-05 00:00:00)."
        ]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_time_based_fill_mean_seconds(self, mock_log_info):
        """
      This test checks the centered rolling fill with second-level frequency using mean.
      """
        checker = DataChecker(self.df_seconds.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mean", time_window="4s")

        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01 00:00:00", periods=6, freq="s"),
            "sensor1": [1, 2, 3, 4.7, 5, 6],
            "sensor2": [2, 2, 3, 4, 3, 2]
        })
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)
        pd.testing.assert_frame_equal(checker.df, expected_df)

        expected_logs = [
            "At timestamp '2025-01-01 00:00:01', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-01 00:00:03).",
            "At timestamp '2025-01-01 00:00:03', filled using mean: 4.7 (window: 2025-01-01 00:00:01 to 2025-01-01 00:00:05).",
            "At timestamp '2025-01-01 00:00:00', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-01 00:00:02).",
            "At timestamp '2025-01-01 00:00:02', filled using mean: 3.0 (window: 2025-01-01 00:00:00 to 2025-01-01 00:00:04).",
            "At timestamp '2025-01-01 00:00:04', filled using mean: 3.0 (window: 2025-01-01 00:00:02 to 2025-01-01 00:00:05)."
        ]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_time_based_fill_sequence(self, mock_log_info):
        """
      This test checks that the method handles a sequence of missing values correctly.
      """
        checker = DataChecker(self.df_sequence.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mean", time_window="2d")

        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=9, freq="d"),
            "sensor1": [1, 2, 3, 4, 5, 6, 6, 6, 6],
            "sensor2": [2, 2, 3, 4, 3, 2, 2, 2, 2]
        })
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)
        pd.testing.assert_frame_equal(checker.df, expected_df)

        expected_logs = [
            "At timestamp '2025-01-02 00:00:00', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-03 00:00:00).",
            "At timestamp '2025-01-04 00:00:00', filled using mean: 4.0 (window: 2025-01-03 00:00:00 to 2025-01-05 00:00:00).",
            "At timestamp '2025-01-07 00:00:00', filled using mean: 6.0 (window: 2025-01-06 00:00:00 to 2025-01-08 00:00:00).",
            "At timestamp '2025-01-08 00:00:00', filled using mean: nan (window: 2025-01-07 00:00:00 to 2025-01-09 00:00:00).",
            "At timestamp '2025-01-09 00:00:00', filled using mean: nan (window: 2025-01-08 00:00:00 to 2025-01-09 00:00:00).",
            "Forward fill applied at the end of column 'sensor1'.",
            "At timestamp '2025-01-01 00:00:00', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-02 00:00:00).",
            "At timestamp '2025-01-03 00:00:00', filled using mean: 3.0 (window: 2025-01-02 00:00:00 to 2025-01-04 00:00:00).",
            "At timestamp '2025-01-05 00:00:00', filled using mean: 3.0 (window: 2025-01-04 00:00:00 to 2025-01-06 00:00:00).",
            "At timestamp '2025-01-08 00:00:00', filled using mean: 2.0 (window: 2025-01-07 00:00:00 to 2025-01-09 00:00:00).",
            "At timestamp '2025-01-09 00:00:00', filled using mean: nan (window: 2025-01-08 00:00:00 to 2025-01-09 00:00:00).",
            "Forward fill applied at the end of column 'sensor2'.",
        ]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_time_based_fill_intermittent(self, mock_log_info):
        """
      This test checks that the method handles intermittent missing values correctly.
      """
        checker = DataChecker(self.df_intermittent.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        checker.handle_missing_values(strategy="fill", fill_method="mean", time_window="2d")

        expected_df = pd.DataFrame({
            "time": pd.date_range("2025-01-01", periods=10, freq="d"),
            "sensor1": [1, 1, 4, 4, 5, 6, 7, 8, 9, 10],
            "sensor2": [2, 2, 3, 4, 5, 6, 7, 8, 9, 9]
        })
        expected_df["sensor1"] = expected_df["sensor1"].astype(float)
        expected_df["sensor2"] = expected_df["sensor2"].astype(float)
        pd.testing.assert_frame_equal(checker.df, expected_df)

        expected_logs = [
            "At timestamp '2025-01-02 00:00:00', filled using mean: 1.0 (window: 2025-01-01 00:00:00 to 2025-01-03 00:00:00).",
            "At timestamp '2025-01-03 00:00:00', filled using mean: 4.0 (window: 2025-01-02 00:00:00 to 2025-01-04 00:00:00).",
            "At timestamp '2025-01-05 00:00:00', filled using mean: 5.0 (window: 2025-01-04 00:00:00 to 2025-01-06 00:00:00).",
            "At timestamp '2025-01-07 00:00:00', filled using mean: 7.0 (window: 2025-01-06 00:00:00 to 2025-01-08 00:00:00).",
            "At timestamp '2025-01-09 00:00:00', filled using mean: 9.0 (window: 2025-01-08 00:00:00 to 2025-01-10 00:00:00).",
            "At timestamp '2025-01-01 00:00:00', filled using mean: 2.0 (window: 2025-01-01 00:00:00 to 2025-01-02 00:00:00).",
            "At timestamp '2025-01-04 00:00:00', filled using mean: 4.0 (window: 2025-01-03 00:00:00 to 2025-01-05 00:00:00).",
            "At timestamp '2025-01-06 00:00:00', filled using mean: 6.0 (window: 2025-01-05 00:00:00 to 2025-01-07 00:00:00).",
            "At timestamp '2025-01-08 00:00:00', filled using mean: 8.0 (window: 2025-01-07 00:00:00 to 2025-01-09 00:00:00).",
            "At timestamp '2025-01-10 00:00:00', filled using mean: 9.0 (window: 2025-01-09 00:00:00 to 2025-01-10 00:00:00)."
        ]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_time_based_fill_invalid_time_window(self, mock_log_error):
        """
      This test checks that an error is raised when an invalid time window is provided.
      """
        checker = DataChecker(self.df_days.copy(), sensors=["sensor1", "sensor2"], time_column="time")
        mock_log_error.side_effect = ValueError("Invalid time_window 'invalid'. Must be a valid Pandas offset string (e.g., '1min', '5min').")
        
        with self.assertRaises(ValueError) as context:
            checker.handle_missing_values(strategy="fill", fill_method="mean", time_window="invalid")
        
        self.assertEqual(str(context.exception), "Invalid time_window 'invalid'. Must be a valid Pandas offset string (e.g., '1min', '5min').")
        mock_log_error.assert_called_once_with("Invalid time_window 'invalid'. Must be a valid Pandas offset string (e.g., '1min', '5min').")

if __name__ == "__main__":
    unittest.main()

