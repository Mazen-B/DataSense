import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.preprocessing.core_preprocessor import DataChecker

class TestDetectOutliers(unittest.TestCase):

    def setUp(self):
        self.df_no_outliers = pd.DataFrame({"sensor_1": [10, 12, 11, 13, 10, 12],
                                            "sensor_2": [5, 6, 5, 6, 5, 6]})

        self.df_with_outliers = pd.DataFrame({"sensor_1": [10, 12, 11, 13, 100, 12],
                                              "sensor_2": [5, 60, 5, 6, 5, 6]})

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_detect_outliers_z_score_no_outliers(self, mock_log_info):
        """
      This test checks that no outliers are detected when data has no significant deviations.
      """
        checker = DataChecker(self.df_no_outliers.copy(), sensors=["sensor_1", "sensor_2"], time_column=None)
        checker.detect_outliers(method="z_score", threshold=3)

        mock_log_info.assert_not_called()

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    def test_detect_outliers_z_score_with_outliers(self, mock_log_info):
        """
      This test checks that outliers are correctly detected using the z-score method.
      """
        checker = DataChecker(self.df_with_outliers.copy(), sensors=["sensor_1", "sensor_2"], time_column=None)
        checker.detect_outliers(method="z_score", threshold=2)

        expected_logs = [
            "Detected 1 outliers in column 'sensor_1' using method 'z_score'.",
            "Detected 1 outliers in column 'sensor_2' using method 'z_score'."]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.logging.info")
    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_detect_outliers_iqr_with_outliers(self, mock_log_warning, mock_log_info):
        """
      This test checks that outliers are correctly detected using the IQR method.
      """
        checker = DataChecker(self.df_with_outliers.copy(), sensors=["sensor_1", "sensor_2"], time_column=None)
        checker.detect_outliers(method="iqr", threshold=1.5)

        expected_logs = [
            "Detected 1 outliers in column 'sensor_1' using method 'iqr'.",
            "Detected 1 outliers in column 'sensor_2' using method 'iqr'."]

        actual_logs = [call.args[0] for call in mock_log_info.call_args_list]
        self.assertEqual(expected_logs, actual_logs)

    @patch("data_manager.preprocessing.core_preprocessor.log_and_raise_error")
    def test_detect_outliers_invalid_method(self, mock_log_error):
        """
      This test checks that an error is raised when an invalid outlier detection method is provided.
      """
        checker = DataChecker(self.df_no_outliers.copy(), sensors=["sensor_1", "sensor_2"], time_column=None)

        mock_log_error.side_effect = ValueError("Unknown outlier detection method 'invalid_method' provided.")
        with self.assertRaises(ValueError) as context:
            checker.detect_outliers(method="invalid_method", threshold=1.5)

        self.assertEqual(str(context.exception), "Unknown outlier detection method 'invalid_method' provided.")
        mock_log_error.assert_called_once_with("Unknown outlier detection method 'invalid_method' provided.")

    @patch("data_manager.preprocessing.core_preprocessor.logging.warning")
    def test_detect_outliers_no_sensor_column(self, mock_log_warning):
        """
      This test checks that a warning is logged if a specified sensor column is not present in the DataFrame.
      """
        checker = DataChecker(self.df_no_outliers.copy(), sensors=["non_existing_sensor"], time_column=None)
        checker.detect_outliers(method="z_score", threshold=3)

        mock_log_warning.assert_called_once_with("Sensor column 'non_existing_sensor' not found in DataFrame.")

if __name__ == "__main__":
    unittest.main()
