from utils.logging_setup import log_and_raise_error
from data_manager.loaders.csv_file_reader import CSVFileReader
from data_manager.loaders.excel_file_reader import ExcelFileReader

def load_data(file_path):
    """
  This function determines if the file is Excel or CSV based on its extension.
  """
    file_path = str(file_path)
    if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
        return ExcelFileReader(file_path)
    elif file_path.endswith(".csv"):
        return CSVFileReader(file_path)
    else:
        log_and_raise_error("Unsupported file format, please choose a csv or excel file")
