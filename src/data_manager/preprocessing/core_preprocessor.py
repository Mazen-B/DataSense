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
        
        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            log_and_raise_error(f"Missing columns in data: {missing_columns}")
        
        # Check for columns that are completely empty
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
                            start_time = max(current_time - window_offset, self.df.index.min())
                            end_time = min(current_time + window_offset, self.df.index.max())

                            if not (start_time <= end_time):
                                return None

                            window_data = self.df.loc[start_time:end_time, column]
                            if window_data.empty:
                                return None

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

    def encode_categorical_and_booleans(self):
        """
      This method encodes non-numeric columns into numerical representations.
        - Boolean-like values (True/False) are converted to 1/0.
        - Binary categorical values (e.g., ON/OFF) are mapped to 1/0.
        - Multi-class categorical values are encoded as integers.
      """
        for column in self.df.columns:
            if column == self.time_column:
                continue
            col_dtype = self.df[column].dtype

            # boolean columns
            if col_dtype == "bool":
                logging.info(f"Encoding boolean column '{column}' as integers.")
                self.df[column] = self.df[column].astype(int)

            # object or string-based columns
            elif col_dtype == "object" or str(col_dtype).startswith("string"):
                unique_values = self.df[column].dropna().unique()
                
                # if binary categorical (e.g., ON/OFF, Yes/No, etc.)
                if len(unique_values) == 2:
                    logging.info(f"Encoding binary categorical column '{column}' as integers.")
                    self.df[column] = self.df[column].map({unique_values[0]: 0, unique_values[1]: 1})
                
                # if multi-class categorical
                else:
                    logging.info(f"Encoding multi-class categorical column '{column}' with unique values: {unique_values}.")
                    self.df[column] = self.df[column].astype("category").cat.codes

            # handle any unexpected data types
            elif not pd.api.types.is_numeric_dtype(col_dtype):
                logging.warning(f"Unexpected non-numeric column '{column}' detected with dtype '{col_dtype}'.")

        return self.df

    def validate_data_types(self):
        """
      This method validates that the columns have expected data types.
      """
        valid_dtypes = ["int64", "float64"]

        # identify non-numeric columns, excluding the time column
        non_numeric_cols = [
            col for col in self.df.columns 
            if col != self.time_column and not pd.api.types.is_numeric_dtype(self.df[col].dtype)]
    
        if non_numeric_cols:
            log_and_raise_error(f"Non-numeric columns detected (excluding time column): {non_numeric_cols}")

        # identify numeric columns that are not of the expected types
        invalid_numeric_cols = [col for col in self.df.columns 
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

