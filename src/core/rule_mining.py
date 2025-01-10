from data_manager.data_processing import DataProcessor

def rule_mining(input_file, output_dir, time_column, time_format, sensors, date_range, core_processing_par, time_processing_par):
    """
  This function handles the common steps required for a single day, specified time range, or full data options. 
  It processes the data, prepares the data, and then generates the stats.  
  """
    # step 1: loads the portion of the data that we need, then process it
    data_processor = DataProcessor(input_file, output_dir, time_column, time_format, sensors, core_processing_par, time_processing_par)
    
    if date_range:
        start_date, end_date = date_range
        data_processor.process_time_range(start_date, end_date)
    else:
        data_processor.process_full_data()

    # further steps will be added later
