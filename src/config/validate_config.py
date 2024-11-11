from datetime import datetime
from utils.logging_setup import log_and_raise_error

ACCEPTED_TIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S"]
ACCEPTED_MODES = ["single_day", "multi_days", "everyday", "full_data"]
REQUIRED_CONFIG_KEYS = ["logging.log_file", "processing.output_dir", "processing.time_column", 
                        "processing.time_format", "processing.mode", "sensors"]

def validate_config(config):
    """
  This function validates the loaded configuration.
  """
    # determine mode-specific required keys
    if config["processing"]["mode"] == "single_day":
        REQUIRED_CONFIG_KEYS.append("processing.date")
    elif config["processing"]["mode"] == "multi_days":
        REQUIRED_CONFIG_KEYS.extend(["processing.start_date", "processing.end_date"])

    # check if any required keys are missing
    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if not nested_key_exists(config, key)]
    if missing_keys:
        log_and_raise_error(f"Missing required config keys: {', '.join(missing_keys)}")

    # validate log_file extension
    log_file = config["logging"]["log_file"]
    if not log_file.endswith((".csv", ".xlsx")):
        log_and_raise_error("Unsupported 'log_file' type: please use CSV or XLSX.")

    # validate output_dir string
    if not isinstance(config["processing"]["output_dir"], str) or not config["processing"]["output_dir"]:
        log_and_raise_error("Invalid 'output_dir' configuration: must be a non-empty string.")

    # validate time_column
    if not isinstance(config["processing"]["time_column"], str) or not config["processing"]["time_column"]:
        log_and_raise_error("Invalid 'time_column' configuration: must be a non-empty string.")

    # validate time_format against accepted formats
    time_format = config["processing"]["time_format"]
    if time_format not in ACCEPTED_TIME_FORMATS:
        log_and_raise_error(f"Invalid 'time_format': please choose from {', '.join(ACCEPTED_TIME_FORMATS)}.")

    # validate mode against accepted modes
    mode = config["processing"]["mode"]
    if mode not in ACCEPTED_MODES:
        log_and_raise_error(f"Invalid 'mode': please choose from {', '.join(ACCEPTED_MODES)}.")

    # validate sensors list
    if not isinstance(config["sensors"], list) or not config["sensors"]:
        log_and_raise_error("Invalid 'sensors' configuration: must be a non-empty list.")

    # validate date for single-day mode
    if mode == "single_day":
        if not isinstance(config["processing"].get("date"), str) or not config["processing"]["date"]:
            log_and_raise_error("Invalid 'date' configuration: must be a non-empty string.")
        validate_date_format(config["processing"]["date"], "date")

    # validate start and end dates for multi-day mode
    if mode == "multi_days":
        if not isinstance(config["processing"].get("start_date"), str) or not config["processing"]["start_date"]:
            log_and_raise_error("Invalid 'start_date' configuration: must be a non-empty string.")
        validate_date_format(config["processing"]["start_date"], "start_date")
        
        if not isinstance(config["processing"].get("end_date"), str) or not config["processing"]["end_date"]:
            log_and_raise_error("Invalid 'end_date' configuration: must be a non-empty string.")
        validate_date_format(config["processing"]["end_date"], "end_date")

def nested_key_exists(config, key):
    """
  This is a helper function to check for nested key existence in a dictionary.
  """
    keys = key.split(".")
    for k in keys:
        if isinstance(config, dict) and k in config:
            config = config[k]
        else:
            return False
    return True

def validate_date_format(date_str, key_name):
    """
  This function checks if date_str matches the "%Y-%m-%d" format.
  """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        log_and_raise_error(f"Invalid '{key_name}' configuration: must be in 'YYYY-MM-DD' format.")