import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class TestDateTimeConversion(unittest.TestCase):

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_exception")
    def test_convert_to_datetime_success(self, mock_log_exception):
        """ 
      This test checks that convert_to_datetime successfully converts valid string dates to datetime 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        preprocessor.convert_to_datetime()

        # check that all values in the column are valid datetime objects
        self.assertTrue(all(isinstance(val, pd.Timestamp) for val in preprocessor.df["time"]))

        # check no exception was raised
        mock_log_exception.assert_not_called()

    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_exception")
    def test_convert_to_datetime_failure(self, mock_log_exception):
        """ 
      This test checks that convert_to_datetime returns "NaT" when an invalid format is provided 
      """
        df = pd.DataFrame({"time": ["invalid_date", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        preprocessor.convert_to_datetime()

        # check that the first row is NaT and other rows are valid datetime
        self.assertTrue(pd.isna(preprocessor.df.loc[0, "time"]))  # NaT for invalid date
        self.assertTrue(isinstance(preprocessor.df.loc[1, "time"], pd.Timestamp))  # valid datetime
        self.assertTrue(isinstance(preprocessor.df.loc[2, "time"], pd.Timestamp))  # valid datetime

        # ensure the exception was not logged
        mock_log_exception.assert_not_called()

    @patch("data_manager.preprocessing.time_preprocessor.logging.warning")
    @patch("data_manager.preprocessing.time_preprocessor.log_and_raise_error")
    def test_handle_failed_datetime_conversion(self, mock_log_error, mock_log_warning):
        """ 
      This test checks that handle_failed_datetime_conversion raises an error and logs a warning when rows with NaT are present after conversion 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "invalid_date_1", "2025-01-03", "invalid_date_2"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")

        # get NaT by converting with errors="coerce"
        preprocessor.convert_to_datetime()

        # ensure the second row was converted to NaT
        self.assertTrue(preprocessor.df["time"].isna().any())

        # test the handling of failed conversion
        mock_log_error.side_effect = ValueError("Failed to convert 2 rows to datetime. Inspect or clean these rows.")
        with self.assertRaises(ValueError) as context:
            preprocessor.handle_failed_datetime_conversion()

        # ensure the correct error message was raised
        self.assertEqual(
            str(context.exception),
            "Failed to convert 2 rows to datetime. Inspect or clean these rows.")

        # ensure a warning was logged about failed conversion
        mock_log_warning.assert_called_once_with("2 rows failed datetime conversion and have been set to NaT.")

    def test_no_failed_datetime_conversion(self):
        """ 
      This test checks that handle_failed_datetime_conversion does nothing when there are no failed conversions 
      """
        df = pd.DataFrame({"time": ["2025-01-01", "2025-01-02", "2025-01-03"]})
        preprocessor = TimePreprocessor(df, "time", "%Y-%m-%d")
        preprocessor.convert_to_datetime()

        # ensure no NaT values are present
        self.assertFalse(preprocessor.df["time"].isna().any())

        # this should not raise an error since there are no NaT values
        preprocessor.handle_failed_datetime_conversion()

if __name__ == "__main__":
    unittest.main()
