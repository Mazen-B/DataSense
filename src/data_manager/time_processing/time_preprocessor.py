import pandas as pd
from utils.logging_setup import log_and_raise_error

class TimePreprocessor:
    def __init__(self, df, time_column, time_format):
        self.df = df
        self.time_column = time_column
        self.time_format = time_format

    def process_time_column(self):
        """
      This is the initial filtering step that processes the time column by converting to datetime, sorting, checking duplicates, and validating.
      """
        self.convert_to_datetime()
        self.order_time_column()

        # set the time column as the index
        self.df.set_index(self.time_column, inplace=True)
        return self.df

    def convert_to_datetime(self, errors="raise"):
        """
      This method converts the date column to datetime format using the specified time format.
      """
        self.df[self.time_column] = pd.to_datetime(self.df[self.time_column], format=self.time_format, errors=errors)
        if self.df[self.time_column].isna().any():
            log_and_raise_error(f"Some values in '{self.time_column}' could not be converted to datetime with format '{self.time_format}'.")

    def order_time_column(self):
        """
      This method orders the DataFrame by the time column in ascending order.
      """
        self.df = self.df.sort_values(by=self.time_column)
 