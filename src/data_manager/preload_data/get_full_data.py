import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error
from data_manager.loaders.data_loader import load_data
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class FullDataLoader:
    def __init__(self, file_path, sensors, time_column, time_format, check_duplicates_keep):
        self.file_path = file_path
        self.sensors = sensors
        self.time_column = time_column
        self.time_format = time_format
        self.check_duplicates_keep = check_duplicates_keep
        self.filtered_time = None
        self.time_data_checker = None

    def get_filtered_data(self):
        """
      This method loads the final filtered data based on start and end rows and required columns.
      """
        # step 1: load all required columns
        columns = [self.time_column] + self.sensors
        data = load_data(self.file_path).read_file(columns)

        # step 2: filter the time column
        self.time_data_checker = TimePreprocessor(data[[self.time_column]], self.time_column, self.time_format)
        self.filtered_time = self.time_data_checker.process_time_column(self.check_duplicates_keep).index.to_series()

        # step 3: apply the mask for the date range
        start_date = pd.to_datetime(data[self.time_column].min() )
        end_date = pd.to_datetime(data[self.time_column].max() )
        data[self.time_column] = pd.to_datetime(data[self.time_column], errors="coerce")

        # create the mask and filter the data
        date_mask = (data[self.time_column] >= start_date) & (data[self.time_column] <= end_date)
        filtered_data = data[date_mask]

        # check if the filtered data is empty
        if filtered_data.empty:
            log_and_raise_error(f"No data found in the specified date range: {start_date} to {end_date}")

        logging.info(f"Extracted date range from {start_date} to {end_date}.")
        logging.info(f"DataFrame was filtered to the needed date range and columns.")
        return filtered_data
