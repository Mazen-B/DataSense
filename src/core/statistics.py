from data_manager.data_processing import DataProcessor

def generate_stats(input_file, output_dir, time_column, time_format, sensors, date, core_processing_par, check_duplicates_keep):
    """
  This function handles the common steps required for single-day, multi-day, and full_data options. 
  It processes the data for the given date, prepares the data, and then generates the stats.  
  """
    # step 1: loads the portion of the data that we need and process the data
    data_processor = DataProcessor(input_file, output_dir, time_column, time_format, sensors, core_processing_par, check_duplicates_keep)
    
    # for debugging
    if date:
        one_day_data = data_processor.process_days(date)
        print("what is one_day_data: ", one_day_data)

    # further steps will be added later
