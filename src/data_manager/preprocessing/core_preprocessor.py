import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error, log_and_raise_exception

class DataChecker:
    def __init__(self, df, sensors=None, time_column=None):
        self.df = df
        self.sensors = sensors if sensors is not None else []
        self.time_column = time_column

    def full_validation(self):
        """
      This method performs a full validation and cleaning process.
      """
        self.validate_columns()
        self.handle_missing_values(strategy="fill", fill_method="mean", fill_value=None, time_window="5min")
        # self.validate_data_types()
        # self.detect_outliers()
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

    def handle_missing_values(self, strategy, fill_method="drop", fill_value=None, time_window=None):
        """
      This method handles missing values in the DataFrame for all columns.
      We can have the follwing args:
        strategy: Strategy to handle missing values, either "drop" or "fill". Default is "drop".
        fill_method: Method to fill missing values, such as "mean", "median", "mode", "constant", or "interpolate".
        fill_value: Value to use if fill_method is "constant".
        time_window: Optional. A Pandas offset string (e.g., "1min", "5min") to specify a rolling window for calculating replacement values.
      """
        # get numeric columns, excluding the time column
        numeric_columns = self.df.select_dtypes(include=["number"]).columns.drop(self.time_column, errors="ignore")

        # additioanl check for NaT values in the time column
        if self.df[self.time_column].isna().any():
            log_and_raise_error(f"Time column '{self.time_column}' contains invalid or missing values (NaT).")

        self.df = self.df.set_index(self.time_column)

        for column in numeric_columns:
            if strategy == "drop":
                missing_count = self.df[column].isna().sum()
                if missing_count > 0:
                    logging.warning(f"{missing_count} missing values found in '{column}', dropping rows.")
                self.df = self.df.dropna(subset=[column])

            elif strategy == "fill":
                missing_count = self.df[column].isna().sum()
                if missing_count > 0:
                    logging.warning(f"{missing_count} missing values found in '{column}', handling with '{fill_method}'.")

                if time_window is None:
                    # apply global replacement
                    if fill_method == "ffill":
                        self.df[column] = self.df[column].fillna(method="ffill")
                    elif fill_method == "bfill":
                        self.df[column] = self.df[column].fillna(method="bfill")
                    elif fill_method == "mean":
                        mean_value = round(self.df[column].mean(), 1)
                        self.df[column] = self.df[column].fillna(mean_value)
                    elif fill_method == "median":
                        median_value = round(self.df[column].median(), 1)
                        self.df[column] = self.df[column].fillna(median_value)
                    elif fill_method == "mode":
                        mode_value = self.df[column].mode()
                        if not mode_value.empty:
                            self.df[column] = self.df[column].fillna(mode_value[0])
                        else:
                            logging.warning(f"Cannot compute mode for column '{column}', skipping.")
                    elif fill_method == "constant":
                        if fill_value is None:
                            log_and_raise_error("No 'fill_value' provided for filling missing values with a constant.")
                        self.df[column] = self.df[column].fillna(fill_value)
                    elif fill_method == "interpolate":
                        self.df[column] = self.df[column].interpolate()
                    else:
                        log_and_raise_error(f"Unknown fill_method '{fill_method}' provided.")
                else:
                    if fill_method in ["mean", "median", "mode"]:
                        window_offset = pd.Timedelta(time_window) / 2

                        # create a custom function to apply
                        def centered_rolling_func(row):
                            current_time = row.name
                            start_time = current_time - window_offset
                            end_time = current_time + window_offset
                            window_data = self.df[column][start_time:end_time]
                            if fill_method == "mean":
                                return round(window_data.mean(), 1)
                            elif fill_method == "median":
                                return round(window_data.median(), 1)
                            elif fill_method == "mode":
                                mode_value = window_data.mode()
                                return mode_value.iloc[0] if not mode_value.empty else None

                        # apply the custom function only to missing values
                        missing_indices = self.df[self.df[column].isna()].index
                        filled_values = self.df.loc[missing_indices].apply(centered_rolling_func, axis=1)
                        self.df.loc[missing_indices, column] = filled_values

                        # check for any remaining missing values in the column
                        remaining_missing = self.df[column].isna().sum()
                        if remaining_missing > 0:
                            log_and_raise_error(f"After filling, {remaining_missing} missing values remain in '{column}' (try increasing the time_window value).")
                    else:
                        log_and_raise_error(f"Unsupported fill_method '{fill_method}' with time_window.")
            else:
                log_and_raise_error(f"Unknown strategy '{strategy}' provided for handling missing values.")

        # reset the index to restore the time column
        self.df = self.df.reset_index()

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
      This method detects outliers in sensor columns based on the specified method.
      """
        for col in self.sensors:
            if col in self.df.columns:
                if method == "z_score":
                    mean = self.df[col].mean()
                    std = self.df[col].std()
                    if std == 0:
                        logging.warning(f"Standard deviation for column '{col}' is zero; cannot compute z-scores.")
                        continue
                    z_scores = (self.df[col] - mean) / std
                    outliers = z_scores.abs() > threshold
                elif method == "iqr":
                    Q1 = self.df[col].quantile(0.25)
                    Q3 = self.df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - (threshold * IQR)
                    upper_bound = Q3 + (threshold * IQR)
                    outliers = (self.df[col] < lower_bound) | (self.df[col] > upper_bound)
                else:
                    log_and_raise_error(f"Unknown outlier detection method '{method}' provided.")
                    continue

                outlier_count = outliers.sum()
                if outlier_count > 0:
                    logging.info(f"Detected {outlier_count} outliers in column '{col}' using method '{method}'.")
                    # TODO: handle outliers (e.g., remove or flag them)
            else:
                logging.warning(f"Sensor column '{col}' not found in DataFrame.")
        return self.df

    def standardize_column_names(self):
        """
      This method standardizes column names by making them lowercase, tripping leading/trailing spaces, and replacing spaces with underscores.
      """
        try:
            if not isinstance(self.df, pd.DataFrame):
                log_and_raise_error("Data format error: 'self.df' is not a DataFrame.")
            
            self.df.columns = (
                self.df.columns.str.strip()  # remove leading/trailing spaces
                            .str.lower()  # convert to lowercase
                            .str.replace(" ", "_", regex=False)  # replace spaces with underscores
            )
        except AttributeError as e:
            log_and_raise_exception(f"Attribute error in 'standardize_column_names': {str(e)}")
        except Exception as e:
            log_and_raise_exception(f"Unexpected error in 'standardize_column_names': {str(e)}")
        
        return self.df
