import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error
from data_manager.loaders.data_loader import load_data
from data_manager.preprocessing.time_preprocessor import TimePreprocessor

class FilterDateRange:
    def __init__(self, file_path, sensors, time_column, time_format):
        self.file_path = file_path
        self.sensors = sensors
        self.time_column = time_column
        self.time_format = time_format
        self.filtered_time = None
        self.time_data_checker = None

    def _initialize_time_column(self):
        """
      This method initializes and filters only the time column using DataChecker.
      """
        # load only the time column from the dataset
        time_data = load_data(self.file_path).read_file(columns=[self.time_column])
        self.time_data_checker = TimePreprocessor(time_data, self.time_column, self.time_format)

        # process the time column + convert it to series
        self.filtered_time = self.time_data_checker.process_time_column().index.to_series()
        
    def _find_date_rows(self, start_date, end_date=None):
        """
      This method finds the start and end rows based on the available timestamps for a specified date or date range.
      If "end_date" is None, it defaults to finding rows within a single day specified by "start_date".
      """
        self._initialize_time_column()

        # convert dates to datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date) if end_date else start_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        # create a mask to select the range within the Series
        date_mask = (self.filtered_time >= start_date) & (self.filtered_time <= end_date)
        matching_rows = self.filtered_time[date_mask]

        # check if any rows match within the specified range
        if matching_rows.empty:
            if end_date == start_date:
                log_and_raise_error(f"No data found for the specified date: {start_date.date()}")
            else:
                log_and_raise_error(f"No data found in the specified date range: {start_date} to {end_date}")

        # get start and end indices for the matching range
        start_row_timestamp  = matching_rows.index.min()
        end_row_timestamp = matching_rows.index.max()

        # get integer indices for start and end rows
        start_row_index = matching_rows.index.get_loc(start_row_timestamp)
        end_row_index = matching_rows.index.get_loc(end_row_timestamp)

        logging.info(f"Extracted date range from {start_row_timestamp} to {end_row_timestamp}.")

        return (start_row_index, end_row_index), matching_rows

    def get_filtered_data(self, start_date, end_date=None):
        """
      This method loads the final filtered data based on start and end rows and required columns.
      """
        # step 1: determine the row range based on the date range
        (start_row_index, end_row_index), filtered_time = self._find_date_rows(start_date, end_date)

        # Step 2: load only the necessary rows and columns based on these indices
        data = load_data(self.file_path).read_file(
            skiprows=range(1, start_row_index),
            nrows=end_row_index - start_row_index + 1,
            columns=self.sensors
        )

        # Step 3: add the time column
        data[self.time_column] = filtered_time.values

        # Step 4: Reorder columns to make the time column the first one
        columns_order = [self.time_column] + [col for col in data.columns if col != self.time_column]
        data = data[columns_order]

        logging.info(f"DataFrame was partially loaded with the needed columns and row range.")
        return data
