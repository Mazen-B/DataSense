import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error

class RuleMiningProcessor:
    def __init__(self, df, sensors_dict, time_column):
        self.df = df
        self.sensors_dict = sensors_dict
        self.time_column = time_column
        self.discretized_info = {}

    def advanced_preprocessing(self, method, bins, labels, continuous_sensor_types):
        """
      This method performs data preprocessing for association rule mining.
      """
        logging.info("Starting the data cleaning and preprocessing process for association rule mining.")
        ordinal_sensors = "ordinal"
        categorical_sensors = "categorical"

        self.discretize_and_encode(continuous_sensor_types, method, bins, labels)
        self.clean_and_encode_ordinal(ordinal_sensors)
        self.convert_categorical_to_bool(categorical_sensors)
        self.last_emptness_check()

        logging.info("Data cleaning and discretization completed for association rule mining.")
        return self.df

    def discretize_and_encode(self, continuous_sensor_types, method, bins, labels):
        """ 
      This method discretizes the continuous columns and one-hot encodes the resulting categories.
      """
        logging.info(f"Starting discretization using method: {method}, bins: {bins}.")

        if continuous_sensor_types:
            df_discretized = self.df.copy()
            
            # get only the columns to discretize (continuous sensors)
            continuous_columns = self._get_sensors_by_type(continuous_sensor_types)
            labels = [f"bin_{i}" for i in range(bins)] if labels is None else labels

            for col in continuous_columns:
                if method == "equal_width":
                    df_discretized[col], bin_edges = pd.cut(
                        self.df[col], bins=bins, labels=labels, retbins=True)
                elif method == "quantile":
                    df_discretized[col], bin_edges = pd.qcut(
                        self.df[col], q=bins, retbins=True, duplicates="drop")
                    num_bins = len(bin_edges) - 1
                    if len(labels) != num_bins:
                        labels = [f"bin_{i}" for i in range(num_bins)]
                    df_discretized[col] = pd.qcut(
                        self.df[col], q=num_bins, labels=labels, retbins=False, duplicates="drop")
                else:
                    log_and_raise_error("Invalid method. Choose 'equal_width' or 'quantile'.")

                self.discretized_info[col] = self._format_bin_info(bin_edges)
            logging.info(f"Discretization completed. Bin information: {self.discretized_info}")

            # one-hot encode the discretized columns
            self.df = pd.get_dummies(df_discretized, columns=continuous_columns)

        else:
            logging.info(f"One-hot encoding skipped for continuous columns, none are specified")

    def clean_and_encode_ordinal(self, ordinal_sensor_types):
        """
      This method removes the time column and one-hot encodes ordinal columns.
      """
        # remove the time column if present
        if self.time_column in self.df.columns:
            self.df.drop(columns=[self.time_column], inplace=True)
            logging.info(f"Removed '{self.time_column}' column from the dataset.")
        if ordinal_sensor_types:
            # get columns to encode (ordinal columns)
            ordinal_columns = self._get_sensors_by_type(ordinal_sensor_types)
            # one-hot encode ordinal columns
            self.df = pd.get_dummies(self.df, columns=ordinal_columns)

            logging.info(f"One-hot encoding completed for ordinal columns: {ordinal_columns}")
        else:
            logging.info(f"One-hot encoding skipped for ordinal columns, none are specified")

    def convert_categorical_to_bool(self, categorical_sensors):
        """
      This method converts categorical columns to boolean (True/False) type. It ensures that columns with
      binary values (0/1) are converted to True/False, which improves compatibility with rule mining algorithms.      
      """
        categorical_columns = self._get_sensors_by_type(categorical_sensors)
        
        # convert columns to boolean type
        self.df[categorical_columns] = self.df[categorical_columns].astype(bool)

        if isinstance(categorical_columns, list) and categorical_columns:
            logging.info(f"Converted categorical columns to boolean: {categorical_columns}")

    def last_emptness_check(self):
        """
      This method checks for any remaining empty values and ensures all columns are binary (0/1).
      """
        empty_values = self.df.isna().sum()
        total_empty = empty_values.sum()

        if total_empty > 0:
            empty_summary = {col: count for col, count in empty_values.items() if count > 0}
            log_and_raise_error(
                f"Data validation failed: {total_empty} empty values remain across {len(empty_summary)} columns. "
                f"Summary: {empty_summary}")
        
        # check if all values in the DataFrame are binary (0 or 1)
        non_binary_columns = [col for col in self.df.columns if not set(self.df[col].unique()).issubset({0, 1})]
        
        if non_binary_columns:
            log_and_raise_error(f"Data validation failed: The following columns contain non-binary values: {non_binary_columns}")
        
        logging.info("Empty value check passed, and all columns are confirmed to be binary.")

    # --- Helper Methods ---
    def _get_sensors_by_type(self, types):
        """
      This helper method extracts the list of sensors by their type.
      """
        sensors_combined = []
        for sensors_division, sensors in self.sensors_dict.items():
            if sensors_division in types:
                if not isinstance(sensors, list):
                    log_and_raise_error(f"Invalid sensor type for division '{sensors_division}': {type(sensors)}. Expected a list.")
                sensors_combined.extend(sensors)
        return sensors_combined

    def _format_bin_info(self, bin_edges, precision=2):
        """
      This helper method formats bin ranges into a readable string with rounded values.
      """
        return [f"bin_{i}: {round(bin_edges[i], precision)} to {round(bin_edges[i+1], precision)}"
                for i in range(len(bin_edges) - 1)]
