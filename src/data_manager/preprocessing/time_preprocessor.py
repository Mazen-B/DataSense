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
        self.validate_time_column()
        self.convert_to_datetime()
        self.order_time_column()

        # set the time column as the index
        self.df.set_index(self.time_column, inplace=True)
        return self.df

    def validate_time_column(self):
        """
      This method validates that the time column is not empty or entirely NaN before processing.
      """
        if self.df.empty:
            log_and_raise_error("The input DataFrame is empty. Please provide a valid DataFrame.")
        
        if self.time_column not in self.df.columns:
            log_and_raise_error(f"The specified time column '{self.time_column}' does not exist in the DataFrame.")
        
        if self.df[self.time_column].isna().any():
            missing_count = self.df[self.time_column].isna().sum()
            log_and_raise_error(f"The time column '{self.time_column}' contains {missing_count} missing (NaN) values. Please clean the data before processing.")

    def convert_to_datetime(self, errors="raise"):
        """
      This method converts the date column to datetime format using the specified time format.
      """
        try:
            self.df[self.time_column] = pd.to_datetime(self.df[self.time_column], format=self.time_format, errors=errors)
        except Exception as e:
            log_and_raise_error(f"Error converting '{self.time_column}' to datetime: {e}")

    def order_time_column(self):
        """
      This method orders the DataFrame by the time column in ascending order.
      """
        self.df = self.df.sort_values(by=self.time_column)
