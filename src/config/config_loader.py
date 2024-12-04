import os
import sys 
import yaml
import logging
from pathlib import Path
from utils.logging_setup import log_and_raise_error
from config.validate_config import validate_config


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.file_management import create_output_dir

def load_validate_config(config_file):
    """
  This function loads and validates the configuration from the YAML file.
  """
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        logging.info("Configuration file %s loaded successfully.", config_file)

        logging.info("Validating configuration...")
        validate_config(config)
        logging.info("Configuration validated successfully.")
        return config

    except FileNotFoundError:
        log_and_raise_error(f"Configuration file {config_file} not found.")
    except yaml.YAMLError as e:
        log_and_raise_error(f"Error parsing YAML config file: {e}")

def get_yaml_input(config, date=True, multi_days=False):
    """
  This function retrieves the needed info from the configuration file.
  """
    log_file = Path(config["logging"]["log_file"])
    output_dir = Path(config["processing"]["output_dir"])
    time_column = config["processing"]["time_column"]
    time_format = config["processing"]["time_format"]
    sensors = config["sensors"]

    # create the output dir if it does not exist
    create_output_dir(output_dir)

    if multi_days:
        start_date = config["processing"]["start_date"]
        end_date = config["processing"]["end_date"]
        return log_file, output_dir, time_column, time_format, sensors, start_date, end_date
    elif date:
        date = config["processing"]["date"]
        return log_file, output_dir, time_column, time_format, sensors, date
    else:
        return log_file, output_dir, time_column, time_format, sensors, None
