import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestEncodeCategoricalAndBooleans(unittest.TestCase):

    def setUp(self):
        self.df_1 = pd.DataFrame({"time": pd.date_range("2025-01-01", periods=6, freq="D"),
                                "boolean_column": [True, False, True, None, False, True]})
        self.df_2 = pd.DataFrame({"time": pd.date_range("2025-01-01", periods=6, freq="D"),
                                "binary_categorical": ["ON", "OFF", "ON", None, "OFF", "ON"]})
        self.df_3 = pd.DataFrame({"time": pd.date_range("2025-01-01", periods=6, freq="D"),
                                "multi_class_categorical": ["A", "B", "C", "A", None, "B"]})
        self.df_4 = pd.DataFrame({"time": pd.date_range("2025-01-01", periods=6, freq="D"),
                                "mixed_type_column": [1, "ON", None, True, "OFF", 0]})        
        
    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_encode_booleans(self, mock_warning):
        """
      This test checks that boolean columns are correctly encoded as integers (1/0).
      """
        checker = DataChecker(self.df_1.copy(), sensors=["boolean_column"], time_column="time")
        checker.encode_categorical_and_booleans()

        expected_df = self.df_1.copy()
        expected_df["boolean_column"] = [1, 0, 1, 1, 0, 1] # None was repaced by 1 (using mode)
        expected_df["boolean_column"] = expected_df["boolean_column"].astype("int8")

        pd.testing.assert_frame_equal(checker.df, expected_df)
        mock_warning.assert_called_once_with("Filling 1 missing values in column 'boolean_column' with mode value 'True'.")

    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_encode_binary_categorical(self, mock_warning):
        """
      This test checks that binary categorical columns are correctly encoded as integers (1/0).
      """
        checker = DataChecker(self.df_2.copy(), sensors=["binary_categorical"], time_column="time")
        checker.encode_categorical_and_booleans()

        expected_df = self.df_2.copy()
        expected_df["binary_categorical"] = [0, 1, 0, 0, 1, 0] # None was repaced by 0 (using mode)
        expected_df["binary_categorical"] = expected_df["binary_categorical"].astype("int8")

        pd.testing.assert_frame_equal(checker.df, expected_df)
        mock_warning.assert_called_once_with("Filling 1 missing values in column 'binary_categorical' with mode value 'ON'.")

    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_encode_multi_class_categorical(self, mock_warning):
        """
      This test checks that multi-class categorical columns are encoded with integer codes.
      """
        checker = DataChecker(self.df_3.copy(), sensors=["multi_class_categorical"], time_column="time")
        checker.encode_categorical_and_booleans()

        expected_df = self.df_3.copy()
        expected_df["multi_class_categorical"] = [0, 1, 2, 0, 0, 1] # None was repaced by 0 (using mode)
        expected_df["multi_class_categorical"] = expected_df["multi_class_categorical"].astype("int8")

        pd.testing.assert_frame_equal(checker.df, expected_df)
        mock_warning.assert_called_once_with("Filling 1 missing values in column 'multi_class_categorical' with mode value 'A'.")

    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_handle_mixed_type_column(self, mock_warning):
        """
      This test checks that mixed-type columns are handled appropriately.
      """
        checker = DataChecker(self.df_4.copy(), sensors=["mixed_type_column"], time_column="time")
        checker.encode_categorical_and_booleans()

        expected_df = self.df_4.copy()
        expected_df["mixed_type_column"] = [1, 3, 1, 1, 2, 0]  # auto-encoded as integers for unique values
        expected_df["mixed_type_column"] = expected_df["mixed_type_column"].astype("int8")

        pd.testing.assert_frame_equal(checker.df, expected_df)

if __name__ == "__main__":
    unittest.main()
