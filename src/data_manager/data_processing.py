from data_manager.preload_data.filter_by_date_range import FilterDateRange
from data_manager.preprocessing.core_preprocessor import DataChecker

class DataProcessor:
    def __init__(self, file_path, sensors, time_column, time_format):
        self.file_path = file_path
        self.sensors = sensors
        self.time_column = time_column
        self.time_format = time_format

    def process_days(self, filtered_data_file, start_date, end_date=None):
        """
      This method prepares the data by loading and validating based on specified date range and columns.
      """
        # Step 1: partially load the data (only the needed columns and rows)
        dates_data_preparer = FilterDateRange(self.file_path, self.sensors, self.time_column, self.time_format)
        filtered_data = dates_data_preparer.get_filtered_data(start_date, end_date)

        # Step 2: preprocess and clean the filtered data
        data_checker = DataChecker(filtered_data, self.sensors, self.time_column)
        filtered_data = data_checker.full_validation()

        # save the filtered data
        filtered_data.to_csv(filtered_data_file, index=False)

        # Step 3: prepare the components needed for plotting (separate them in the conf file)
        time = filtered_data[self.time_column]
        te_sensors = [filtered_data[col] for col in filtered_data.columns if col.startswith("te_")]
        pe_sensors = [filtered_data[col] for col in filtered_data.columns if col.startswith("pe_")]
        other_columns = {col: filtered_data[col] for col in filtered_data.columns if col not in te_sensors + pe_sensors}

        return time, te_sensors, pe_sensors, other_columns

    def process_full_data(self):
        """
      This method prepares and returns the full dataset after initial filtering.
      """
        pass
