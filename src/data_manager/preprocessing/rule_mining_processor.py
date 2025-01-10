import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error

class RuleMiningProcessor:
    def __init__(self, df, sensors_dict):
        self.df = df
        self.sensors_dict = sensors_dict
        self.discretized_info = {}

    def advanced_preprocessing(self, method, bins, labels):
        """
      This method performs data preprocessing for association rule mining.
      """
        logging.info("Starting the data cleaning and preprocessing process for association rule mining.")

        self.df = self.discretize_columns(method, bins, labels)
        self.last_emptness_check()

        logging.info("Data cleaning and discretization completed for association rule mining.")
        return self.df

    def discretize_columns(self, method, bins, labels=None):
        """
      This method discretizes the continuous columns and logs the bin ranges.
      """
        logging.info(f"Starting discretization using method: {method}, bins: {bins}.")
        df_discretized = self.df.copy()
        columns = self._get_needed_sensors(self.sensors_dict)
        labels = [f"bin_{i}" for i in range(bins)] if labels is None else labels

        for col in columns:
            if method == "equal_width":
                df_discretized[col], bin_edges = pd.cut(
                    self.df[col], bins=bins, labels=labels, retbins=True)
            elif method == "quantile":
                df_discretized[col], bin_edges = pd.qcut(
                    self.df[col], q=bins, labels=labels, retbins=True, duplicates="drop")
            else:
                log_and_raise_error("Invalid method. Choose 'equal_width' or 'quantile'.")

            self.discretized_info[col] = self._format_bin_info(bin_edges)

        logging.info(f"Discretization completed. Bin information: {self.discretized_info}")
        return df_discretized

    def last_emptness_check(self):
        """
      This method checks for any remaining empty values in the DataFrame.
      """
        empty_values = self.df.isna().sum()
        total_empty = empty_values.sum()

        if total_empty > 0:
            empty_summary = {col: count for col, count in empty_values.items() if count > 0}
            log_and_raise_error(
                f"Data validation failed: {total_empty} empty values remain across {len(empty_summary)} columns. "
                f"Summary: {empty_summary}")

    # --- Helper Methods ---
    def _get_needed_sensors(self, sensors_dict):
        """
      This helper method extracts the list of sensors to be discretized.
      """
        sensors_combined = []
        for sensors_division, sensors in sensors_dict.items():
            if sensors_division in ["temperature", "pressure", "el_power", "rpm"]:
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
