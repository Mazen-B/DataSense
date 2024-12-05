import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error
from data_manager.loaders.base_file_reader import BaseFileReader

class CSVFileReader(BaseFileReader):
    def read_file(self, columns=None, skiprows=None, nrows=None):
        """
      This method reads a CSV file with options to select columns and limit rows.
      """
        try:
            data = pd.read_csv(self.file_path, usecols=columns, skiprows=skiprows, nrows=nrows)
            if skiprows is None and nrows is None:
                logging.info(f"Successfully read CSV file: {self.file_path} with only the '{columns[0]}' column.")
            else:
                logging.info(f"Successfully read the CSV with specified column(s): {columns}, for the extracted time range.")

            return data

        except Exception as e:
            log_and_raise_error(f"Failed to read CSV file {self.file_path}: {e}")