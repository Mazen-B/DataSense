import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error, log_and_raise_exception

class DataChecker:
    def __init__(self, df, sensors=None, time_column=None):
        self.df = df
        self.sensors = sensors if sensors is not None else []
        self.time_column = time_column

    def full_validation(self, core_processing_par):
        """
      This method performs a full validation and cleaning process.
      """
        logging.info("Starting the data cleaning and validation process for the loaded dataset.")
        
        strategy, fill_method, fill_value, time_window, outliers_method, threshold = core_processing_par

        self.standardize_column_names()
        self.validate_columns()
        self.handle_missing_values(strategy, fill_method, fill_value, time_window)
        self.encode_categorical_and_booleans()
        self.validate_data_types()
        self.detect_outliers(outliers_method, threshold)
        self.last_emptness_check()

        logging.info("Data cleaning and validation process completed. The dataset is now ready for further analysis.")
        return self.df

    def standardize_column_names(self):
        """
      This method standardizes column names by making them lowercase, tripping leading/trailing spaces, 
      and replacing spaces with underscores.
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
            log_and_raise_error(f"Attribute error in 'standardize_column_names': {str(e)}")
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
        
        # check for missing columns
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            log_and_raise_error(f"Missing columns in data: {missing_columns}")
        
        # check for columns that are completely empty
        empty_columns = [col for col in required_columns if self.df[col].isna().all()]
        if empty_columns:
            log_and_raise_error(f"The following columns are completely empty: {empty_columns}")

        return self.df

    def handle_missing_values(self, strategy, fill_method, fill_value=None, time_window=None):
        """
      This method handles missing values in the DataFrame for all columns.
      We can have the follwing args:
        strategy: Strategy to handle missing values, either "drop" or "fill".
        fill_method: Method to fill missing values, such as "ffill", "bfill", "mean", "median", "mode", "constant", or "interpolate".
        fill_value: Value to use if fill_method is "constant".
        time_window: Optional. A Pandas offset string (e.g., "1min", "5min") to specify a rolling window for calculating replacement values.
      """
        self._validate_time_column()
        self.df = self.df.set_index(self.time_column)

        numeric_columns = self._get_numeric_columns()

        if strategy == "drop":
            self._drop_missing_values(numeric_columns)
        elif strategy == "fill":
            self._fill_missing_values(numeric_columns, fill_method, fill_value, time_window)
        else:
            log_and_raise_error(f"Unknown strategy '{strategy}' provided for handling missing values.")

        # reset index to restore the time column
        self.df = self.df.reset_index()

        return self.df

    def encode_categorical_and_booleans(self):
        """
        This method encodes non-numeric columns into numerical representations.
        - Boolean-like values (True/False) are converted to 1/0.
        - Binary categorical values (e.g., ON/OFF) are mapped to 1/0.
        - Multi-class categorical values are encoded as integers.
        - Missing values in non-numeric columns are filled with the mode.
        """
        for column in self.df.columns:
            if column == self.time_column:
                continue

            # check for missing values and fill with mode if necessary
            missing_count = self.df[column].isna().sum()
            if missing_count > 0:
                mode_value = self.df[column].mode()
                if not mode_value.empty:
                    logging.warning(f"Filling {missing_count} missing values in column '{column}' with mode value '{mode_value.iloc[0]}'.")
                    self.df[column] = self.df[column].fillna(mode_value.iloc[0])
                else:
                    logging.warning(f"Cannot compute mode for column '{column}' due to empty or invalid data; missing values remain.")

            col_dtype = self.df[column].dtype

            # boolean columns
            if col_dtype == "bool":
                logging.info(f"Encoding boolean column '{column}' as integers (0/1).")
                self.df[column] = self.df[column].astype("int8")

            # object or string-based columns
            elif col_dtype == "object" or str(col_dtype).startswith("string"):
                unique_values = self.df[column].dropna().unique()

                # binary categorical (e.g., ON/OFF, Yes/No)
                if len(unique_values) == 2:
                    logging.info(
                        f"Encoding binary categorical column '{column}' as integers (0/1) with mapping: "
                        f"{{'{unique_values[0]}': 0, '{unique_values[1]}': 1}}.")
                    self.df[column] = (self.df[column].map({unique_values[0]: 0, unique_values[1]: 1}).astype("int8"))

                # multi-class categorical
                else:
                    logging.info(f"Encoding multi-class categorical column '{column}' with unique values: {unique_values}.")
                    self.df[column] = self.df[column].astype("category").cat.codes.astype("int8")

                    for idx, value in enumerate(unique_values):
                        logging.info(f"Encoded value '{value}' as {idx} in column '{column}'.")

            # handle any unexpected non-numeric data types
            elif not pd.api.types.is_numeric_dtype(col_dtype):
                logging.warning(f"Unexpected non-numeric column '{column}' detected with dtype '{col_dtype}'. No encoding applied.")

        return self.df

    def validate_data_types(self):
        """
      This method validates that the columns have expected data types.
      """
        valid_dtypes = ["int8", "int16", "int32", "int64", "float32", "float64"]

        # identify non-numeric columns, excluding the time column
        non_numeric_cols = [
            col for col in self.df.columns 
            if col != self.time_column and not pd.api.types.is_numeric_dtype(self.df[col].dtype)]
    
        if non_numeric_cols:
            log_and_raise_error(f"Non-numeric columns detected (excluding time column): {non_numeric_cols}")

        # identify numeric columns that are not of the expected types
        invalid_numeric_cols = [
            col for col in self.df.columns
            if col != self.time_column and str(self.df[col].dtype) not in valid_dtypes]
        
        if invalid_numeric_cols:
            log_and_raise_error(f"Numeric columns with unexpected types detected (not in {valid_dtypes}): {invalid_numeric_cols}")

        return self.df

    def detect_outliers(self, method, threshold):
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

    def last_emptness_check(self):
        """
      This method checks for any remaining empty values in the DataFrame after the cleaning process.
      If any empty values are found, it raises an error with details of the affected columns and timestamps.
      """
        empty_values = self.df.isna().sum()
        total_empty = empty_values.sum()
        
        if total_empty > 0:
            empty_details = {
                col: self.df.loc[self.df[col].isna(), self.time_column].tolist()
                for col in empty_values[empty_values > 0].index}

            log_and_raise_error(f"Data validation failed: {total_empty} empty values remain. Details: {empty_details}")

    # --- Helper Methods ---
    def _validate_time_column(self):
        """
      This helper method validates the time column for missing or invalid values.
      """
        if self.time_column and self.df[self.time_column].isna().any():
            log_and_raise_error(f"Time column '{self.time_column}' contains invalid or missing values (NaT).")

    def _get_numeric_columns(self):
        """
      This helper method returns numeric columns, excluding the time column.
      """
        return self.df.select_dtypes(include=["number"]).columns.drop(self.time_column, errors="ignore")

    def _drop_missing_values(self, columns):
        """
      This helper method drops rows with missing values in the specified columns.
      """
        missing_counts = self.df[columns].isna().sum()
        total_missing_rows = self.df[columns].isna().any(axis=1).sum()
        if total_missing_rows > 0:
            missing_rows_timestamps = self.df[self.df[columns].isna().any(axis=1)].index.tolist()
            logging.warning(
                f"Found {total_missing_rows} rows to be dropped with missing values in these columns: {missing_counts.to_dict()}. "
                f"for the following timestamps: {missing_rows_timestamps}.")
            self.df = self.df.dropna(subset=columns)

    def _fill_missing_values(self, columns, fill_method, fill_value, time_window):
        """
      This helper method fills missing values in the specified columns using the given method.
      """
        # additional checks for the fill method and time window
        accepted_methods = ["ffill", "bfill", "mean", "median", "mode", "constant", "interpolate"]
        if fill_method not in accepted_methods:
            log_and_raise_error(f"Invalid fill method '{fill_method}' provided.")

        if time_window is not None:
            try:
                pd.Timedelta(time_window)
            except ValueError:
                log_and_raise_error(f"Invalid time_window '{time_window}'. Must be a valid Pandas offset string (e.g., '1min', '5min').")

        for column in columns:
            missing_count = self.df[column].isna().sum()
            if missing_count == 0:
                continue
            
            missing_rows_timestamps = self.df[self.df[column].isna()].index.tolist()
            logging.warning(
                f"{missing_count} missing values found in '{column}', handling with '{fill_method}'. "
                f"Timestamps of missing rows: {missing_rows_timestamps}.")

            try:
                if time_window is None:
                    self._apply_global_fill(column, fill_method, fill_value)
                elif time_window is not None and fill_method in ["mean", "median"]:
                    self._apply_time_based_fill(column, fill_method, time_window)
                else:
                    log_and_raise_error(f"Unsupported combination of fill_method '{fill_method}' and time_window '{time_window}' for column '{column}'.")
            except Exception as e:
                logging.error(f"Failed to handle missing values in column '{column}' using method '{fill_method}': {e}")

    def _apply_global_fill(self, column, fill_method, fill_value):
        """
      This helper method fills missing values globally using the specified fill method.
      """
        if fill_method == "ffill":
            self.df[column] = self.df[column].ffill()
            # check if any NaN remains at the start and apply bfill if needed
            if self.df[column].isna().iloc[0]:
                logging.warning(f"Missing value at the start of column '{column}' after forward fill. Applying backward fill for the first value.")
                self.df[column] = self.df[column].bfill()

        elif fill_method == "bfill":
            self.df[column] = self.df[column].bfill()
            # check if any NaN remains at the end and apply ffill if needed
            if self.df[column].isna().iloc[-1]:
                logging.warning(f"Missing value at the end of column '{column}' after backward fill. Applying forward fill for the last value.")
                self.df[column] = self.df[column].ffill()

        elif fill_method == "mean":
            self.df[column] = self.df[column].fillna(self.df[column].mean())

        elif fill_method == "median":
            self.df[column] = self.df[column].fillna(self.df[column].median())

        elif fill_method == "mode":
            mode_value = self.df[column].mode()
            if not mode_value.empty:
                self.df[column] = self.df[column].fillna(mode_value[0])
                if len(mode_value) > 1:
                    logging.warning(f"Multiple modes found for column '{column}', using the first mode: {mode_value[0]}.")
            else:
                logging.warning(f"Cannot compute mode for column '{column}', skipping.")

        elif fill_method == "constant":
            if fill_value is None:
                log_and_raise_error("No 'fill_value' provided for filling missing values with a constant.")
            self.df[column] = self.df[column].fillna(fill_value)

        elif fill_method == "interpolate":
            self.df[column] = self.df[column].interpolate()
            if self.df[column].isna().iloc[0]:  # check for NaN at the start
                logging.warning(f"Missing value at the start of column '{column}' after interpolation. Applying forward bfill.")
                self.df[column] = self.df[column].bfill()
            if self.df[column].isna().iloc[-1]:  # check for NaN at the end
                logging.warning(f"Missing value at the end of column '{column}' after interpolation. Applying backward ffill.")
                self.df[column] = self.df[column].ffill()
        else:
            log_and_raise_error(f"Unknown fill_method '{fill_method}' provided.")

    def _apply_time_based_fill(self, column, fill_method, time_window):
        """
      This helper method fills missing values based on a centered rolling time window.
      """
        window_offset = pd.Timedelta(time_window) / 2

        def centered_rolling(row):
            current_time = row.name
            start_time = max(current_time - window_offset, self.df.index.min())
            end_time = min(current_time + window_offset, self.df.index.max())

            if not (start_time <= end_time):
                logging.info(f"At timestamp '{current_time}', no valid window found. Skipping.")
                return None

            window_data = self.df.loc[start_time:end_time, column]
            if window_data.empty:
                logging.info(f"At timestamp '{current_time}', window is empty. Skipping.")
                return None

            if fill_method == "mean":
                new_value = round(window_data.mean(), 1)
                logging.info(f"At timestamp '{current_time}', filled using mean: {new_value} (window: {start_time} to {end_time}).")
                return new_value
            elif fill_method == "median":
                new_value = round(window_data.median(), 1)
                logging.info(f"At timestamp '{current_time}', filled using median: {new_value} (window: {start_time} to {end_time}).")
                return new_value

        try:
            # apply centered rolling only to missing values
            missing_indices = self.df[self.df[column].isna()].index
            filled_values = self.df.loc[missing_indices].apply(centered_rolling, axis=1)
            self.df.loc[missing_indices, column] = filled_values

            # handle remaining missing values at the start and end
            if self.df[column].isna().iloc[0]:
                logging.warning(f"Remaining missing values at the start of column '{column}' after centered rolling fill. Applying backward fill.")
                self.df[column] = self.df[column].bfill()
                logging.info(f"Backward fill applied at the start of column '{column}'.")

            if self.df[column].isna().iloc[-1]:
                logging.warning(f"Remaining missing values at the end of column '{column}' after centered rolling fill. Applying forward fill.")
                self.df[column] = self.df[column].ffill()
                logging.info(f"Forward fill applied at the end of column '{column}'.")

            # global fill for any remaining missing values
            remaining_missing_global_fill = self.df[column].isna().sum()
            if remaining_missing_global_fill > 0:
                global_fill_value = self.df[column].mean() if fill_method == "mean" else self.df[column].median()
                logging.warning(f"{remaining_missing_global_fill} missing values remain in column '{column}' after boundary fills. Applying global {fill_method} fill.")
                self.df[column] = self.df[column].fillna(global_fill_value)
                logging.info(f"Global {fill_method} fill applied with value: {global_fill_value}.")

            # final check that returns an error for any remaining missing values
            remaining_missing = self.df[column].isna().sum()
            if remaining_missing > 0:
                log_and_raise_error(f"After filling, {remaining_missing} missing values remain in '{column}'. Try increasing the time_window value.")

        except Exception as e:
            logging.error(f"Error in time-based fill for column '{column}': {e}")
            raise
