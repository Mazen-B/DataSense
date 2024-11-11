import logging
from pathlib import Path
from data_manager.data_processing import DataProcessor

def get_yaml_input(config, date=True, multi_days=False):
    """
  This function retrieves the needed info from the configuration file.
  """
    log_file = Path(config["logging"]["log_file"])
    output_dir = Path(config["processing"]["output_dir"])
    time_column = config["processing"]["time_column"]
    time_format = config["processing"]["time_format"]
    sensors = config["sensors"]

    if multi_days:
        start_date = config["processing"]["start_date"]
        end_date = config["processing"]["end_date"]
        return log_file, output_dir, time_column, time_format, sensors, start_date, end_date
    elif date:
        date = config["processing"]["date"]
        return log_file, output_dir, time_column, time_format, sensors, date
    else:
        return log_file, output_dir, time_column, time_format, sensors, None

def generate_stats(log_file, output_dir, time_column, time_format, sensors, date):
    """
  This function handles the common steps required for single-day, multi-day, everyday, and full_data options. 
  It processes the data for the given date, prepares the data, and then generates the stats.  
  """
    # step 1: process the data using DataProcesso
    data_processor = DataProcessor(log_file, sensors, time_column, time_format)
    one_day_data = data_processor.process_days(date)
    
    print("what is one_day_data: ", one_day_data)

    # further steps will be added later

def run_single_day(config):
    """
  This function runs statistics for a single day.
  """
    # get input from the config
    log_file, output_dir, time_column, time_format, sensors, date = get_yaml_input(config, date=True)

    logging.info(f"Starting single_day analysis for {date}")
    generate_stats(log_file, output_dir, time_column, time_format, sensors, date)
  
def run_multi_days(config):
    pass

def run_everyday(config):
    pass

def run_full_data(config):
    pass
