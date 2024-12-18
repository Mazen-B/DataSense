import os
from data_manager.preload_data.filter_by_date_range import DataLoader
from data_manager.preprocessing.core_preprocessor import DataChecker

class DataProcessor:
    def __init__(self, input_file, output_dir, time_column, time_format, sensors, core_processing_par, check_duplicates_keep):
        self.input_file = input_file
        self.output_dir = output_dir
        self.time_column = time_column
        self.time_format = time_format
        self.sensors_dict = sensors
        self.core_processing_par = core_processing_par
        self.check_duplicates_keep = check_duplicates_keep

    def process_days(self, start_date, end_date=None):
        """
      This method prepares the data by loading and validating based on specified date range and columns.
      """
        # get the sensors from the their divisions 
        sensors = []
        for sensors_division in self.sensors_dict.values():
            if not isinstance(sensors_division, list):
                raise ValueError(f"Invalid sensor division type: {type(sensors_division)}. Expected a list.")
            sensors.extend(sensors_division)
            
        # Step 1: partially load the data (returns only the needed columns and rows)
        dates_data_preparer = DataLoader(self.input_file, sensors, self.time_column, self.time_format, self.check_duplicates_keep)
        filtered_data = dates_data_preparer.get_filtered_data(start_date, end_date)

        # Step 2: preprocess and clean the filtered data
        data_checker = DataChecker(filtered_data, sensors, self.time_column)
        processed_data = data_checker.full_validation(self.core_processing_par)
        
        # save the processed data
        processed_data_file = os.path.join(self.output_dir, "processed_data.csv")
        processed_data.to_csv(processed_data_file, index=False)

        # Step 3: prepare the components needed for EDA
        time = processed_data[self.time_column]
        organized_sensors = {}
        for division, division_sensors in self.sensors_dict.items():
            organized_sensors[division] = [processed_data[col] for col in division_sensors if col in processed_data.columns]
        
        return time, organized_sensors

    def process_full_data(self):
        """
      This method prepares and returns the full dataset after initial filtering.
      """
        pass
