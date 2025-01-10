import pandas as pd
from datetime import datetime
from utils.logging_setup import log_and_raise_error

ACCEPTED_TIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S"]
REQUIRED_CONFIG_KEYS = ["input_file", "output_dir", "time_column", "time_format", "sensors", "pre_processing"]

def validate_config(config):
    """
  This function validates the configuration and dynamically set the mode based on the date inputs.
  """
    validate_date_inputs(config)
    validate_common_config(config)

def validate_date_inputs(config):
    """
  This function validates the date-related fields and dynamically set the mode.
  """
    date = config.get("date")
    start_date = config.get("start_date")
    end_date = config.get("end_date")

    if bool(start_date) != bool(end_date):  # XOR check for start_date and end_date
        log_and_raise_error("Both 'start_date' and 'end_date' must be specified together if one is provided.")

    if date:
        config["mode"] = "single_day"
        validate_date_format(date, "date")
    elif start_date and end_date:
        config["mode"] = "time_range"
        validate_date_format(start_date, "start_date")
        validate_date_format(end_date, "end_date")
    else:
        config["mode"] = "full_data"

def validate_common_config(config):
    """
  This function validates non-date-related configuration fields.
  """
    # check for missing required keys
    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if not nested_key_exists(config, key)]
    if missing_keys:
        log_and_raise_error(f"Missing required config keys: {', '.join(missing_keys)}")

    # validate input_file
    input_file = config["input_file"]
    if not input_file.endswith((".csv", ".xlsx")):
        log_and_raise_error("Invalid 'input_file': must be a CSV or XLSX file.")

    # validate output_dir
    output_dir = config["output_dir"]
    if not isinstance(output_dir, str) or not output_dir.strip():
        log_and_raise_error("Invalid 'output_dir': must be a non-empty string.")

    # validate time_column
    if not isinstance(config["time_column"], str) or not config["time_column"]:
        log_and_raise_error("Invalid 'time_column' configuration: must be a non-empty string.")

    # validate time_format
    time_format = config["time_format"]
    if time_format not in ACCEPTED_TIME_FORMATS:
        log_and_raise_error(f"Invalid 'time_format': must be one of {', '.join(ACCEPTED_TIME_FORMATS)}.")

    # validate sensors
    sensors = config.get("sensors", {})
    validate_sensors(sensors)

    # validate pre_processing
    pre_processing = config.get("pre_processing", {})
    validate_pre_processing(pre_processing)

def nested_key_exists(config, key):
    """
  This function checks if a nested key exists in the configuration dictionary.
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
  This function validates the format of date strings.
  """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        log_and_raise_error(f"Invalid '{key_name}' format: must be 'YYYY-MM-DD'.")

def validate_sensors(sensors):
    """
  This function validates the sensors section of the configuration.
  """
    allowed_divisions = ["temperature", "pressure", "el_power", "rpm", "ordinal", "categorical"]
    if not sensors:
        log_and_raise_error("Invalid 'sensors': At least one division with sensors must be provided.")

    invalid_divisions = [key for key in sensors if key not in allowed_divisions]
    if invalid_divisions:
        log_and_raise_error(f"Invalid 'sensors' divisions: {invalid_divisions}. Allowed: {allowed_divisions}.")

    for division, sensors_list in sensors.items():
        if not isinstance(sensors_list, list) or not sensors_list:
            log_and_raise_error(f"Invalid 'sensors' division '{division}': must contain a non-empty list of sensors.")

def validate_pre_processing(pre_processing):
    """
  This function validates the pre_processing section of the configuration.
  """
    if not pre_processing:
        log_and_raise_error("Invalid 'pre_processing': make sure all the needed divisions are filled.")
    handle_missing_values = pre_processing.get("handle_missing_values", {})
    validate_handle_missing_values(handle_missing_values)

    detect_outliers = pre_processing.get("detect_outliers", None)
    if detect_outliers:
        validate_detect_outliers(detect_outliers)

    time_col_config = pre_processing.get("time_col", {})
    validate_time_col(time_col_config)

def validate_handle_missing_values(hmv_config):
    """
  This function validates the handle_missing_values section.
  """
    valid_strategies = ["drop", "fill"]
    valid_fill_methods = ["ffill", "bfill", "mean", "median", "mode", "constant", "interpolate"]

    strategy = hmv_config.get("strategy")
    if strategy not in valid_strategies:
        log_and_raise_error(f"Invalid 'strategy': must be one of {valid_strategies}.")

    if strategy == "fill":
        fill_method = hmv_config.get("fill_method")
        if fill_method not in valid_fill_methods:
            log_and_raise_error(f"Invalid 'fill_method': must be one of {valid_fill_methods}.")
        if fill_method == "constant" and not hmv_config.get("fill_value"):
            log_and_raise_error("Invalid 'fill_value': must be provided when 'fill_method' is 'constant'.")

    if "time_window" in hmv_config:
        try:
            pd.Timedelta(hmv_config["time_window"])
        except ValueError:
            log_and_raise_error("Invalid 'time_window': must be a valid pandas offset string.")

def validate_detect_outliers(do_config):
    """
  This function validates the detect_outliers section.
  """
    valid_methods = ["z_score", "iqr"]
    if do_config.get("method") not in valid_methods:
        log_and_raise_error(f"Invalid 'method': must be one of {valid_methods}.")

    if "threshold" in do_config and not isinstance(do_config["threshold"], (int, float)):
        log_and_raise_error("Invalid 'threshold': must be a numeric value.")

def validate_time_col(time_col_config):
    """
  This function validates the time_col section in the config
  """
    validation_rules = {
        "check_duplicates_keep": ["first", "last", None],
        "handle_missing_values": ["error", "drop"],
        "failed_datetime_conversion": ["error", "drop"]
    }
    
    for key, valid_options in validation_rules.items():
        value = time_col_config.get(key)
        if value not in valid_options:
            log_and_raise_error(f"Invalid '{key}': must be one of {valid_options}.")
