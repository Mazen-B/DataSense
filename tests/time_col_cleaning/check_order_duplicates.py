import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class TestOrderAndCheckDuplicates(unittest.TestCase):

    def test_order_time_column(self):
        """ 
      This test checks that order_time_column correctly sorts the DataFrame by the time column 
      """
        df = pd.DataFrame({"time": ["2025-01-03", "2025-01-01", "2025-01-02"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.order_time_column()

        # check if the DataFrame is sorted in ascending order by the time column
        expected_order = ["2025-01-01", "2025-01-02", "2025-01-03"]
        self.assertListEqual(preprocessor.df["time"].tolist(), expected_order)

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_check_duplicates_no_duplicates(self, mock_log_error):
        """ 
      This test checks that check_duplicates does nothing when there are no duplicates 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.check_duplicates(keep="first")

        # ensure the DataFrame remains unchanged
        self.assertEqual(len(preprocessor.df), 3)

        # ensure log_and_raise_error was not called
        mock_log_error.assert_not_called()

    @patch("data_manager.preprocessing.time_preprocessor.logging.warning")
    def test_check_duplicates_keep_first(self, mock_log_warning):
        """ 
      This test checks that check_duplicates removes duplicates and keeps the first occurrence when keep='first' 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.check_duplicates(keep="first")

        # ensure duplicates were removed and first occurrence was kept
        expected_df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        pd.testing.assert_frame_equal(preprocessor.df.reset_index(drop=True), expected_df)

        # ensure a warning was logged about duplicates
        mock_log_warning.assert_called_once_with("1 duplicate rows were found in the 'time' column and removed. Duplicated time values: ['2025-01-02']")

    @patch("data_manager.preprocessing.time_preprocessor.logging.warning")
    def test_check_duplicates_keep_last(self, mock_log_warning):
        """ 
      This test checks that check_duplicates removes duplicates and keeps the last occurrence when keep='last' 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03", "2025-01-02"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.check_duplicates(keep="last")

        # ensure duplicates were removed and last occurrence was kept
        expected_df = pd.DataFrame({"time": ["2025-01-01", "2025-01-03", "2025-01-02"]})
        pd.testing.assert_frame_equal(preprocessor.df.reset_index(drop=True), expected_df)

        # ensure a warning was logged about duplicates
        mock_log_warning.assert_called_once_with("2 duplicate rows were found in the 'time' column and removed. Duplicated time values: ['2025-01-02']")

    @patch("data_manager.preprocessing.time_preprocessor.logging.warning")
    def test_check_duplicates_keep_none(self, mock_log_warning):
        """ 
      This test checks that check_duplicates logs a warning but does not remove duplicates when keep=None 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.check_duplicates(keep=None)

        # ensure the DataFrame remains unchanged
        self.assertEqual(len(preprocessor.df), 4)

        # ensure a warning was logged about duplicates
        mock_log_warning.assert_called_once_with(
            "Duplicates in column 'time' were found but not removed as 'keep=None' was specified. "
            "Duplicated time values: ['2025-01-02']")

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_check_duplicates_missing_column(self, mock_log_error):
        """ 
      This test checks that check_duplicates raises an error when the specified time column is missing 
      """
        df = pd.DataFrame({"date": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        mock_log_error.side_effect = ValueError("Column 'time' not found in DataFrame.")

        with self.assertRaises(ValueError) as context:
            preprocessor.check_duplicates(keep="first")

        self.assertEqual(str(context.exception), "Column 'time' not found in DataFrame.")
        mock_log_error.assert_called_once_with("Column 'time' not found in DataFrame.")

if __name__ == "__main__":
    unittest.main()
