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

def get_yaml_input(config, single_date=False, time_range=False):
    """
  This function retrieves the needed info from the config file.
  """
    input_file = Path(config["input_file"])
    output_dir = Path(config["output_dir"])
    time_column = config["time_column"]
    time_format = config["time_format"]

    # get the sensors
    sensors = {}
    ALLOWED_DIVISIONS = ["temperature", "pressure", "el_power", "rpm", "ordinal", "categorical"]
    for division in ALLOWED_DIVISIONS:
        if division in config["sensors"]:
            sensors[division] = config["sensors"][division]

    # get pre-processing parameters
    pre_processing = config["pre_processing"]
    missing_values_strategy = pre_processing["handle_missing_values"]["strategy"]
    missing_values_fill_method = pre_processing["handle_missing_values"]["fill_method"]
    missing_values_fill_value = pre_processing["handle_missing_values"]["fill_value"]
    missing_values_time_window = pre_processing["handle_missing_values"]["time_window"]
    detect_outliers_method = pre_processing.get("detect_outliers", {}).get("method")
    detect_outliers_threshold = pre_processing.get("detect_outliers", {}).get("threshold")
    check_duplicates_keep = pre_processing["time_col"]["check_duplicates_keep"]
    time_col_missing_values = pre_processing["time_col"]["handle_missing_values"]
    time_col_datetime_conversion = pre_processing["time_col"]["failed_datetime_conversion"]
    core_processing_par = [missing_values_strategy, missing_values_fill_method, missing_values_fill_value,
        missing_values_time_window, detect_outliers_method, detect_outliers_threshold]
    time_processing_par = [check_duplicates_keep, time_col_missing_values, time_col_datetime_conversion]

    # get rule mining parameters if present
    rule_mining_config = pre_processing.get("rule_mining", None)
    if rule_mining_config:
        rule_mining_method = rule_mining_config.get("method")
        rule_mining_bins = rule_mining_config.get("bins")
        rule_mining_labels = rule_mining_config.get("labels")
        continuous_sensor_types = rule_mining_config.get("continuous_sensor_types", [])
        ordinal_sensor_types = rule_mining_config.get("ordinal_sensor_types", [])
        rule_mining_processing_par = [rule_mining_method, rule_mining_bins, rule_mining_labels, continuous_sensor_types]
    else:
        rule_mining_processing_par = None

    # create the output dir if it does not exist
    create_output_dir(output_dir)

    if single_date:
        # for "single_day" mode
        date = config["date"]
        return input_file, output_dir, time_column, time_format, sensors, date, core_processing_par, time_processing_par, rule_mining_processing_par
    elif time_range:
        # for "time_range" mode 
        start_date = config["start_date"]
        end_date = config["end_date"]
        return input_file, output_dir, time_column, time_format, sensors, start_date, end_date, core_processing_par, time_processing_par, rule_mining_processing_par
    else:
        # for "full_data" mode 
        return input_file, output_dir, time_column, time_format, sensors, core_processing_par, time_processing_par, rule_mining_processing_par
