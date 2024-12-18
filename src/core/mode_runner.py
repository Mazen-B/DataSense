import os
import sys
import logging
from core.statistics import generate_stats

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config_loader import get_yaml_input

def run_single_day(config):
    """
  This function runs statistics for a single day.
  """
    # get input from the config
    input_file, output_dir, time_column, time_format, sensors, date, core_processing_par, check_duplicates_keep = get_yaml_input(config, date=True)

    logging.info(f"Starting analysis for one day {date}.")
    generate_stats(input_file, output_dir, time_column, time_format, sensors, date, core_processing_par, check_duplicates_keep)
  
def run_multi_days(config):
    pass

def run_everyday(config):
    pass
