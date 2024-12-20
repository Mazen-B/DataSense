import logging
import pandas as pd
from utils.logging_setup import log_and_raise_error
from data_manager.loaders.base_file_reader import BaseFileReader

class ExcelFileReader(BaseFileReader):
    def read_file(self, columns=None, skiprows=None, nrows=None):
        """
      This method reads an Excel file and logs the action. Raises an error if reading fails.
      """
        try:
            data = pd.read_excel(self.file_path, usecols=columns, skiprows=skiprows, nrows=nrows)
            if not skiprows and not nrows and len(columns) == 1:
                logging.info(f"Successfully read Excel file: {self.file_path} with only the '{columns[0]}' column.")
            else:
                logging.info(f"Successfully read the Excel CSV with specified column(s): {columns}, for the extracted time range.")
            return data

        except Exception as e:
            log_and_raise_error(f"Failed to read Excel file {self.file_path}: {e}")
