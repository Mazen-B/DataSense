import os
import sys
import unittest
import tempfile
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.prepare_data.get_full_data import FullDataLoader
from data_manager.loaders.csv_file_reader import CSVFileReader
from data_manager.loaders.excel_file_reader import ExcelFileReader
from data_manager.loaders.data_loader import load_data

class TestFullDataLoader(unittest.TestCase):

    def setUp(self):
        self.time_column = "time"
        self.sensors = ["sensor_1", "sensor_2"]
        self.time_format = "%Y-%m-%d %H:%M:%S"
        self.check_duplicates_keep = "first"
        self.temp_dir = tempfile.TemporaryDirectory()

        # create dummy dataset
        dummy_data = pd.DataFrame({
            "time": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-02 00:00:00", "2025-01-02 11:00:00", "2025-01-03 00:00:00"]),
            "sensor_1": [10, 20, 30, 40, 50],
            "sensor_2": [100, 200, 300, 400, 500]
        })
        self.csv_file_path = os.path.join(self.temp_dir.name, "dummy_dataset.csv")
        dummy_data.to_csv(self.csv_file_path, index=False)

        # create empty dataset
        self.empty_dataset = os.path.join(self.temp_dir.name, "empty_dataset.csv")
        empty_data = pd.DataFrame(columns=["time", "sensor_1", "sensor_2"])
        empty_data.to_csv(self.empty_dataset, index=False)

    def tearDown(self):
        # cleanup temporary directory and files
        self.temp_dir.cleanup()

    def test_full_load_with_correct_data(self):
        """ 
      This test checks that FullDataLoader correctly loads the entire dataset with the expected columns.
      """
        # mock the data returned by load_data
        mock_data = pd.DataFrame({
            "time": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-02 00:00:00", "2025-01-02 11:00:00", "2025-01-03 00:00:00"]),
            "sensor_1": [10, 20, 30, 40, 50],
            "sensor_2": [100, 200, 300, 400, 500]
        })

        loader = FullDataLoader(self.csv_file_path, self.sensors, self.time_column, self.time_format, self.check_duplicates_keep)
        filtered_data = loader.get_filtered_data()

        # check if the loaded data matches the mock data
        pd.testing.assert_frame_equal(filtered_data, mock_data)

    def test_get_filtered_data_empty_dataframe(self):
        """
      This test checks that get_filtered_data raises an error when the input DataFrame is empty.
      """
        loader = FullDataLoader(self.empty_dataset, self.sensors, self.time_column, self.time_format, self.check_duplicates_keep)

        with self.assertRaises(ValueError) as context:
            loader.get_filtered_data()

        # check that the raised error contains the expected message
        self.assertIn("The input DataFrame is empty. Please provide a valid DataFrame.", str(context.exception))

    def test_load_data_csv_reader(self):
        """ 
      This test checks that load_data correctly uses CSVFileReader for CSV files.
      """
        result = load_data("dummy_dataset.csv")
        self.assertIsInstance(result, CSVFileReader)

    def test_load_data_excel_reader(self):
        """ 
      This test checks that load_data correctly uses ExcelFileReader for Excel files.
      """
        result = load_data("dummy_dataset.xlsx")
        self.assertIsInstance(result, ExcelFileReader)

    def test_load_data_unsupported_file_format(self):
        """ 
      This test checks that load_data raises an error for unsupported file formats.
      """
        with self.assertRaises(ValueError) as context:
            load_data("dummy_dataset.txt")

        self.assertIn("Unsupported file format, please choose a csv or excel file", str(context.exception))

if __name__ == "__main__":
    unittest.main()
