import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error, log_and_raise_exception

import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error, log_and_raise_exception

class DataChecker:
    def __init__(self, df, sensors=None, time_column=None):
        self.df = df
        self.sensors = sensors if sensors is not None else []
        self.time_column = time_column

    def handle_missing_values(self, column, strategy="drop"):
        """
      This method handles missing values in the DataFrame based on a specific column.
      """
        if column not in self.df.columns:
            log_and_raise_error(f"Column '{column}' not found in DataFrame.")
        
        if strategy == "drop":
            missing_count = self.df[column].isna().sum()
            if missing_count > 0:
                logging.warning(f"{missing_count} missing values found in '{column}', dropping rows.")
            self.df = self.df.dropna(subset=[column])
        elif strategy == "fill":
            self.df[column] = self.df[column].fillna(method="ffill")
            if self.df[column].isna().any():
                log_and_raise_error(f"Unable to forward fill all missing values in '{column}'.")
        else:
            log_and_raise_error(f"Unknown strategy '{strategy}' provided for handling missing values.")
        return self.df

    def check_duplicates(self, column, keep="first"):
        """
      This method checks for and removes duplicates based on a specific column.
      """
        if column not in self.df.columns:
            log_and_raise_error(f"Column '{column}' not found in DataFrame.")
        
        duplicate_count = self.df.duplicated(subset=[column]).sum()
        if duplicate_count > 0:
            logging.warning(f"{duplicate_count} duplicate rows found based on '{column}', removing duplicates.")
            try:
                self.df = self.df.drop_duplicates(subset=[column], keep=keep)
            except Exception as e:
                log_and_raise_exception(f"Failed to remove duplicates based on '{column}': {str(e)}")
        return self.df

    def standardize_column_names(self):
        """
      This method standardizes column names by making them lowercase and replacing spaces with underscores.
      """
        try:
            if not isinstance(self.df, pd.DataFrame):
                log_and_raise_error("Data format error: 'self.df' is not a DataFrame.")
            self.df.columns = self.df.columns.str.lower().str.replace(" ", "_", regex=False)
        except AttributeError as e:
            log_and_raise_exception(f"Attribute error in 'standardize_column_names': {str(e)}")
        except Exception as e:
            log_and_raise_exception(f"Unexpected error in 'standardize_column_names': {str(e)}")
        return self.df

    def validate_columns(self):
        """
      This method checks if the DataFrame contains the required columns.
      """
        required_columns = self.sensors.copy()
        if self.time_column:
            required_columns.append(self.time_column)
        
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            log_and_raise_error(f"Missing columns in data: {missing_columns}")
        return self.df

    def validate_data_types(self):
        """
      This method validates that the columns have expected data types.
      """
        expected_dtypes = {sensor: "float64" for sensor in self.sensors}
        if self.time_column:
            expected_dtypes[self.time_column] = "datetime64[ns]"
        
        mismatches = {}
        for col, expected_dtype in expected_dtypes.items():
            if col in self.df.columns:
                actual_dtype = str(self.df[col].dtype)
                if actual_dtype != expected_dtype:
                    mismatches[col] = {'expected': expected_dtype, 'actual': actual_dtype}
            else:
                mismatches[col] = {'expected': expected_dtype, 'actual': 'Column not found'}
        
        if mismatches:
            log_and_raise_error(f"Data type mismatches found: {mismatches}")
        return self.df

    def detect_outliers(self, method="z_score", threshold=3):
        """
      This method detects outliers in sensor columns based on z-score or other methods.
      """
        for col in self.sensors:
            if col in self.df.columns:
                if method == "z_score":
                    mean = self.df[col].mean()
                    std = self.df[col].std()
                    if std == 0:
                        logging.warning(f"Standard deviation for column '{col}' is zero, cannot compute z-scores.")
                        continue
                    z_scores = (self.df[col] - mean) / std
                    outliers = z_scores.abs() > threshold
                    outlier_count = outliers.sum()
                    if outlier_count > 0:
                        logging.info(f"Detected {outlier_count} outliers in column '{col}'.")
                        # later: we can handle outliers here (e.g., remove or flag them)
            else:
                logging.warning(f"Sensor column '{col}' not found in DataFrame.")
        return self.df

    def check_time_continuity(self):
        """
      This method ensures that the time column is sorted and has no gaps.
      """
        if self.time_column and self.time_column in self.df.columns:
            if not self.df[self.time_column].is_monotonic_increasing:
                log_and_raise_error(f"The '{self.time_column}' column is not sorted.")
            
            time_deltas = self.df[self.time_column].diff().dropna().unique()
            if len(time_deltas) > 1:
                logging.warning("Gaps detected in the time column.")
        else:
            log_and_raise_error(f"Time column '{self.time_column}' not found in DataFrame.")
        return self.df

    def full_validation(self):
        """
      This method performs a full validation and cleaning process.
      """
        self.standardize_column_names()
        self.validate_columns()
        self.handle_missing_values(column=self.time_column, strategy="drop")
        self.check_duplicates(column=self.time_column)
        self.validate_data_types()
        self.check_time_continuity()
        self.detect_outliers()
        return self.df
