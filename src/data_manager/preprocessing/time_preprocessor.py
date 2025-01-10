import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error, log_and_raise_exception

class TimePreprocessor:
    def __init__(self, df, time_column, time_format):
        self.df = df
        self.time_column = time_column
        self.time_format = time_format

    def process_time_column(self, time_processing_par):
        """
      This is the initial filtering step that processes the time column by converting to datetime, sorting, checking duplicates, and validating.
      """
        keep = time_processing_par[0]
        handle_missing = time_processing_par[1]
        action = time_processing_par[2]
        
        logging.info(f"Starting the initial filtering and processing of the '{self.time_column}' column.")

        self.validate_time_column()
        self.handle_missing_values(handle_missing)
        self.convert_to_datetime()
        self.handle_failed_datetime_conversion(action)
        self.order_time_column()
        self.check_duplicates(keep)

        logging.info(f"The '{self.time_column}' column has been processed, indexed, and converted into a time-series.")
        return self.df

    def validate_time_column(self):
        """
      This method validates that the time column exists and contains valid data types.
      Allows empty values (NaN), but ensures the column's non-empty values have supported types.
      """
        if self.df.empty:
            log_and_raise_error("The input DataFrame is empty. Please provide a valid DataFrame.")
        
        if self.time_column not in self.df.columns:
            log_and_raise_error(f"The specified time column '{self.time_column}' does not exist in the DataFrame.")
        
        non_empty_values = self.df[self.time_column].dropna()
        if not non_empty_values.empty:
            if not pd.api.types.is_string_dtype(non_empty_values) and \
            not pd.api.types.is_datetime64_any_dtype(non_empty_values) and \
            not pd.api.types.is_numeric_dtype(non_empty_values):
                log_and_raise_error(f"The time column '{self.time_column}' contains unsupported data types. "
                                    "Supported types are string, datetime, or numeric.")

    def handle_missing_values(self, method):
        """
      This method handles missing values in the time column based on the specified method (error or drop).
      """
        if self.df[self.time_column].isna().any():
            missing_count = self.df[self.time_column].isna().sum()
            if method == "drop":
                logging.warning(f"Dropping {missing_count} rows with missing values in the '{self.time_column}' column.")
                self.df = self.df.dropna(subset=[self.time_column])
            elif method == "error":
                log_and_raise_error(f"The time column '{self.time_column}' contains {missing_count} missing (NaN) values. "
                                    "Please clean the data or set 'handle_missing' to 'drop'.")
            else:
                log_and_raise_error(f"Invalid method: '{method}' for handling missing values in time column preprocessing, allowed method: error or drop.")               

    def convert_to_datetime(self, errors="coerce"):
        """
      This method converts the date column to datetime format using the specified time format.
      """
        try:
            self.df.loc[:, self.time_column] = pd.to_datetime(self.df[self.time_column], format=self.time_format, errors=errors)        
        except Exception as e:
            log_and_raise_exception(f"Error converting '{self.time_column}' to datetime: {e}")

    def handle_failed_datetime_conversion(self, action):
        """
      This method handles rows where datetime conversion failed (set to NaT). Logs details and drops or raises an error.
      """
        failed_count = self.df[self.time_column].isna().sum()
        if failed_count > 0:
            failed_rows = self.df[self.df[self.time_column].isna()]
            logging.warning(f"{failed_count} rows failed datetime conversion and have been set to NaT.")
            logging.debug(f"Rows with failed datetime conversion: {failed_rows}")
            
            if action == "drop":
                self.df = self.df.dropna(subset=[self.time_column])
                logging.info(f"Dropped {failed_count} rows with failed datetime conversion.")
            elif action == "error":
                log_and_raise_error(f"Failed to convert {failed_count} rows to datetime. Inspect or clean these rows.")
            else:
                raise ValueError("Invalid action specified. Use 'error' or 'drop'.")

    def order_time_column(self):
        """
      This method orders the DataFrame by the time column in ascending order.
      """
        self.df = self.df.sort_values(by=self.time_column)

    def check_duplicates(self, keep):
        """
      This method checks for and removes duplicates based on a specific column.
      We can have the follwing options:
        "keep" (str or None): Determines which duplicates to keep:
            - "first": Keeps the first occurrence of each duplicate and removes others.
            - "last": Keeps the last occurrence of each duplicate and removes others.
            - None: Does not remove duplicates; only logs a warning if duplicates exist.
      """
        if self.time_column not in self.df.columns:
            log_and_raise_error(f"Column '{self.time_column}' not found in DataFrame.")

        duplicate_count = self.df.duplicated(subset=[self.time_column]).sum()
        if duplicate_count > 0:
            # get duplicated time values
            duplicated_times = self.df.loc[self.df.duplicated(subset=[self.time_column], keep=False), self.time_column].unique()
            
            if keep is None:
                # issue a warning with detailed duplicate time values
                logging.warning(f"Duplicates in column '{self.time_column}' were found but not removed as 'keep=None' was specified. "
                                f"Duplicated time values: {list(duplicated_times)}")
                return self.df
            else:
                try:
                    # remove duplicates based on the "keep" parameter
                    logging.warning(f"{duplicate_count} duplicate rows were found in the '{self.time_column}' column and removed. "
                                    f"Duplicated time values: {list(duplicated_times)}")
                    self.df = self.df.drop_duplicates(subset=[self.time_column], keep=keep)
                except Exception as e:
                    log_and_raise_exception(f"Failed to remove duplicates based on '{self.time_column}': {str(e)}")
        return self.df
