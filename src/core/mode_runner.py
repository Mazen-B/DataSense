import os
import sys
import logging
from core.statistics import generate_stats

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config_loader import get_yaml_input

def run_analysis(config, mode):
    """
  This is the main function to handle analysis for all modes.
  """
    input_file, output_dir, time_column, time_format, sensors, date_range, core_processing_par, time_processing_par = prepare_inputs(config, mode)

    if mode == "single_day":
        logging.info(f"Starting analysis for one day: {date_range[0]}.")
    elif mode == "time_range":
        logging.info(f"Starting analysis from {date_range[0]} to {date_range[1]}.")
    else:  
        # full_data
        logging.info("Starting analysis for full data.")

    generate_stats(input_file, output_dir, time_column, time_format, sensors, date_range, core_processing_par, time_processing_par)

def prepare_inputs(config, mode):
    """
  This function prepares inputs based on the mode.
  """
    if mode == "single_day":
        input_file, output_dir, time_column, time_format, sensors, date, core_processing_par, time_processing_par = get_yaml_input(config, single_date=True)
        return input_file, output_dir, time_column, time_format, sensors, (date, None), core_processing_par, time_processing_par
    elif mode == "time_range":
        input_file, output_dir, time_column, time_format, sensors, start_date, end_date, core_processing_par, time_processing_par = get_yaml_input(config, time_range=True)
        return input_file, output_dir, time_column, time_format, sensors, (start_date, end_date), core_processing_par, time_processing_par
    else:  
        # full_data
        input_file, output_dir, time_column, time_format, sensors, core_processing_par, time_processing_par = get_yaml_input(config)
        return input_file, output_dir, time_column, time_format, sensors, None, core_processing_par, time_processing_par
