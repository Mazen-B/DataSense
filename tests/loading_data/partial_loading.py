import os
import sys
import unittest
import tempfile
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
from data_manager.prepare_data.filter_by_date_range import PartialDataLoader

class TestPartialDataLoaderWithFile(unittest.TestCase):

    def setUp(self):
        self.time_column = "time"
        self.sensors = ["sensor_1", "sensor_2"]
        self.time_format="%Y-%m-%d %H:%M:%S"
        self.time_processing_par = ["first", "drop", "error"]
        self.temp_dir = tempfile.TemporaryDirectory()

        # create dummy dataset
        dummy_data = pd.DataFrame({
            "time": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-02 00:00:00", "2025-01-02 11:00:00", "2025-01-03 00:00:00"]),
            "sensor_1": [10, 20, 30, 40, 50],
            "sensor_2": [100, 200, 300, 400, 500]
        })
        self.file_path = os.path.join(self.temp_dir.name, "dummy_dataset.csv")
        dummy_data.to_csv(self.file_path, index=False)
  
    def tearDown(self):
        # cleanup temporary directory and files
        self.temp_dir.cleanup()


    def test_partial_load_with_time_range(self):
        """ 
      This test checks that get_filtered_data correctly loads partial data with the right number of rows and columns for a specified time range.
      """
        loader = PartialDataLoader(self.file_path, self.sensors, self.time_column, self.time_format, self.time_processing_par)
        filtered_data = loader.get_filtered_data(start_date="2025-01-01", end_date="2025-01-02") # end at 2025-01-02 00:00:00 

        self.assertEqual(len(filtered_data), 3)  # should have 3 rows
        self.assertEqual(len(filtered_data.columns), 3)  # time + 2 sensors

        # check that the time column is correctly aligned
        expected_times = pd.Series(pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-02 00:00:00"]), name="time")
        pd.testing.assert_series_equal(filtered_data["time"], expected_times)

    def test_partial_load_for_one_day(self):
        """ 
      This test checks that get_filtered_data correctly loads partial data with the right number of rows and columns  for a specific day.
      """
        loader = PartialDataLoader(self.file_path, self.sensors, self.time_column, self.time_format, self.time_processing_par)
        filtered_data = loader.get_filtered_data(start_date="2025-01-02", end_date=None)

        self.assertEqual(len(filtered_data), 2)  # should have 2 rows
        self.assertEqual(len(filtered_data.columns), 3)  # time + 2 sensors

        # check that the time column is correctly aligned
        expected_times = pd.Series(pd.to_datetime(["2025-01-02 00:00:00", "2025-01-02 11:00:00"]), name="time")
        pd.testing.assert_series_equal(filtered_data["time"], expected_times)

    def test_no_matching_date(self):
        """ 
      This test checks that get_filtered_data raises an error when no data matches the specified date range 
      """
        loader = PartialDataLoader(self.file_path, self.sensors, self.time_column, self.time_format, self.time_processing_par)

        with self.assertRaises(ValueError) as context:
            loader.get_filtered_data(start_date="2025-01-04")

        # ensure the error message is correct
        self.assertIn("No data found in the specified date range: 2025-01-04 00:00:00 to 2025-01-04 23:59:59", str(context.exception))

    def test_no_matching_time_range(self):
        """ 
      This test checks that get_filtered_data raises an error when no data matches for a time range 
      """
        loader = PartialDataLoader(self.file_path, self.sensors, self.time_column, self.time_format, self.time_processing_par)

        with self.assertRaises(ValueError) as context:
            loader.get_filtered_data(start_date="2025-01-04", end_date="2025-01-02")

        # ensure the error message is correct
        self.assertIn("Invalid date range: start_date 2025-01-04 00:00:00 is greater than end_date 2025-01-02 00:00:00", str(context.exception))

    def test_output_values(self):
        """ 
      This test checks that get_filtered_data correctly loads the right rows and all their values for a specified date range.
      """
        loader = PartialDataLoader(self.file_path, self.sensors, self.time_column, self.time_format, self.time_processing_par)
        filtered_data = loader.get_filtered_data(start_date="2025-01-02", end_date="2025-01-03")

        self.assertEqual(len(filtered_data), 3)  # should have 3 rows
        self.assertEqual(len(filtered_data.columns), 3)  # time + 2 sensors

        expected_data = pd.DataFrame({
            "time": pd.to_datetime(["2025-01-02 00:00:00", "2025-01-02 11:00:00", "2025-01-03 00:00:00"]),
            "sensor_1": [30, 40, 50],
            "sensor_2": [300, 400, 500]
        })

        # check that the filtered data matches the expected data
        pd.testing.assert_frame_equal(filtered_data.reset_index(drop=True), expected_data)

if __name__ == "__main__":
    unittest.main()
