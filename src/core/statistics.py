import os
import sys
import logging
from data_manager.data_processing import DataProcessor

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config_loader import get_yaml_input

def generate_stats(log_file, output_dir, time_column, time_format, sensors, date):
    """
  This function handles the common steps required for single-day, multi-day, everyday, and full_data options. 
  It processes the data for the given date, prepares the data, and then generates the stats.  
  """
    # step 1: process the data using DataProcesso
    data_processor = DataProcessor(log_file, sensors, time_column, time_format)
    # specify the file, where we want to store the filtered data
    filtered_data_file = os.path.join(output_dir, "filtered_data.csv")
    one_day_data = data_processor.process_days(filtered_data_file, date)
    

    # further steps will be added later

def run_single_day(config):
    """
  This function runs statistics for a single day.
  """
    # get input from the config
    log_file, output_dir, time_column, time_format, sensors, date = get_yaml_input(config, date=True)

    logging.info(f"Starting single_day analysis for {date}.")
    generate_stats(log_file, output_dir, time_column, time_format, sensors, date)
  
def run_multi_days(config):
    pass

def run_everyday(config):
    pass

def run_full_data(config):
    pass
