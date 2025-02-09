import os
from data_manager.prepare_data.get_full_data import FullDataLoader
from data_manager.preprocessing.core_preprocessor import DataChecker
from data_manager.prepare_data.filter_by_date_range import PartialDataLoader

class DataProcessor:
    def __init__(self, input_file, output_dir, time_column, time_format, sensors, core_processing_par, time_processing_par):
        self.input_file = input_file
        self.output_dir = output_dir
        self.time_column = time_column
        self.time_format = time_format
        self.sensors_dict = sensors
        self.core_processing_par = core_processing_par
        self.time_processing_par = time_processing_par

    def _get_sensors(self):
        """
      This method gets the sensors from their divisions.
      """
        sensors_combined = []
        for sensors_division in self.sensors_dict.values():
            if not isinstance(sensors_division, list):
                raise ValueError(f"Invalid sensor division type: {type(sensors_division)}. Expected a list.")
            sensors_combined.extend(sensors_division)
        
        return sensors_combined

    def _organize_sensors(self, processed_data):
        """
      This method organizes sensors data by division
      """
        organized_sensors = {}
        for division, division_sensors in self.sensors_dict.items():
            organized_sensors[division] = [processed_data[col] for col in division_sensors if col in processed_data.columns]
        return organized_sensors

    def _save_processed_data(self, processed_data):
        """
      This method saves the processed data
      """
        processed_data_file = os.path.join(self.output_dir, "processed_data.csv")
        processed_data.to_csv(processed_data_file, index=False)
    
    def process_time_range(self, start_date, end_date=None):
        """
      This method prepares the data by loading and validating based on specified date range and columns.
      """
        # step 1: partially load the data (returns only the needed columns and rows)
        sensors_combined = self._get_sensors()
        dates_data_preparer = PartialDataLoader(self.input_file, sensors_combined, self.time_column, self.time_format, self.time_processing_par)
        filtered_data = dates_data_preparer.get_filtered_data(start_date, end_date)

        # step 2: preprocess and clean the filtered data
        data_checker = DataChecker(filtered_data, sensors_combined, self.time_column)
        processed_data = data_checker.full_validation(self.core_processing_par)
        
        # save the processed data
        self._save_processed_data(processed_data)

        # step 3: prepare the components needed for further analysis
        time = processed_data[self.time_column]
        organized_sensors = self._organize_sensors(processed_data)

        return time, organized_sensors, processed_data

    def process_full_data(self):
        """
      This method prepares the data by loading only the specified columns for the full dataset after initial filtering.
      """
        # step 1: load the data (for only the needed columns)
        sensors_combined = self._get_sensors()
        dates_data_preparer = FullDataLoader(self.input_file, sensors_combined, self.time_column, self.time_format, self.time_processing_par)
        filtered_data = dates_data_preparer.get_filtered_data()

        # step 2: preprocess and clean the filtered data
        data_checker = DataChecker(filtered_data, sensors_combined, self.time_column)
        processed_data = data_checker.full_validation(self.core_processing_par)
        
        # save the processed data
        self._save_processed_data(processed_data)

        # step 3: prepare the components needed for further analysis
        time = processed_data[self.time_column]
        organized_sensors = self._organize_sensors(processed_data)
        
        return time, organized_sensors, processed_data
