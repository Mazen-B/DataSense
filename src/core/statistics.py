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
