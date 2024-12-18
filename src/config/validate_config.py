import pandas as pd
from datetime import datetime
from utils.logging_setup import log_and_raise_error

ACCEPTED_MODES = ["single_day", "multi_days", "full_data"]
ACCEPTED_TIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S"]
REQUIRED_CONFIG_KEYS = ["input_file", "output_dir", "time_column", "time_format", "mode", "sensors", "pre_processing"]

def validate_config(config):
    """
  This function validates the loaded configuration.
  """
    # determine mode-specific required keys
    date_fields = []
    if config["mode"] == "single_day":
        date_fields.append("date")
    elif config["mode"] == "multi_days":
        date_fields.extend(["start_date", "end_date"])

    # check if any required keys are missing
    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if not nested_key_exists(config, key)]
    if missing_keys:
        log_and_raise_error(f"Missing required config keys: {', '.join(missing_keys)}")

    # validate log_file extension
    log_file = config["input_file"]
    if not log_file.endswith((".csv", ".xlsx")):
        log_and_raise_error("Unsupported 'log_file' type: please use CSV or XLSX.")

    # validate output_dir string
    if not isinstance(config["output_dir"], str) or not config["output_dir"]:
        log_and_raise_error("Invalid 'output_dir' configuration: must be a non-empty string.")

    # validate date format
    if date_fields:
        validate_date_fields(config, date_fields)

    # validate mode against accepted modes
    mode = config["mode"]
    if mode not in ACCEPTED_MODES:
        log_and_raise_error(f"Invalid 'mode': please choose from {', '.join(ACCEPTED_MODES)}.")

    # validate time_column
    if not isinstance(config["time_column"], str) or not config["time_column"]:
        log_and_raise_error("Invalid 'time_column' configuration: must be a non-empty string.")

    # validate time_format against accepted formats
    time_format = config["time_format"]
    if time_format not in ACCEPTED_TIME_FORMATS:
        log_and_raise_error(f"Invalid 'time_format': please choose from {', '.join(ACCEPTED_TIME_FORMATS)}.")

    # validate sensors section
    sensors = config.get("sensors", {})
    validata_sensors(sensors)

    # validate pre_processing section
    pre_processing = config.get("pre_processing", {})

    # validate handle_missing_values
    hmv_config = pre_processing.get("handle_missing_values", {})
    validate_handle_missing_values(hmv_config)
    
    # validate detect_outliers (can be None)
    do_config = pre_processing.get("detect_outliers", None)
    if do_config is not None:
        validate_detect_outliers(do_config)

    # validate check_duplicates
    cd_config = pre_processing.get("check_duplicates", {})
    validate_check_duplicates(cd_config)

    # validate 
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

def validate_date_fields(config, date_fields):
    for field in date_fields:
        if field in config:
            try:
                datetime.strptime(config[field], "%Y-%m-%d")
            except ValueError:
                log_and_raise_error(f"Invalid format for '{field}': must be in 'YYYY-MM-DD' format.")

def validate_date_format(date_str, key_name):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        log_and_raise_error(f"Invalid '{key_name}' configuration: must be in 'YYYY-MM-DD' format.")

def validata_sensors(sensors):
    allowed_divisions = ["temperature", "pressure", "el_power", "rpm", "ordinal", "categorical"]
    active_divisions = [division for division in sensors if division in allowed_divisions]

    # check if sensors are provided
    if not sensors:
        log_and_raise_error("Invalid 'sensors' configuration: At least one division is required, and it must have at least one sensor.")

    # ensure all divisions in the sensors are from the allowed list
    invalid_divisions = [division for division in sensors if division not in allowed_divisions]
    if invalid_divisions:
        log_and_raise_error(f"Invalid 'sensors' configuration: The following divisions are not allowed: {invalid_divisions}. Allowed divisions are: {allowed_divisions}.")

    # check if at least one valid division is provided
    if not active_divisions:
        log_and_raise_error(f"Invalid 'sensors' configuration: At least one of these divisions must be provided: {allowed_divisions}.")

    # check if each division has at least one sensor
    for division in active_divisions:
        if not isinstance(sensors[division], list) or not sensors[division]:
            log_and_raise_error(f"Invalid 'sensors' configuration: Division '{division}' must contain at least one sensor.")

def validate_handle_missing_values(hmv_config):
    valid_strategies = ["drop", "fill"]
    valid_fill_methods = ["mean", "median", "mode", "constant", "interpolate"]

    # check if strategy is empty
    if "strategy" not in hmv_config or hmv_config["strategy"] is None:
        log_and_raise_error("Invalid 'handle_missing_values': 'strategy' must have a value.")

    # check if strategy has a valid option
    if "strategy" not in hmv_config or hmv_config["strategy"] not in valid_strategies:
        log_and_raise_error("Invalid 'strategy' in handle_missing_values: choose 'drop' or 'fill'.")

    # check if the fill strategy has a valid option
    if hmv_config["strategy"] == "fill":
        fill_method = hmv_config.get("fill_method")
        if fill_method not in valid_fill_methods:
            log_and_raise_error(f"Invalid 'fill_method': choose from {', '.join(valid_fill_methods)}.")

        if fill_method == "constant":
            fill_value = hmv_config.get("fill_value")
            if fill_value is None or not isinstance(fill_value, str) or not fill_value.strip():
                log_and_raise_error("'fill_value' must be specified as a non-empty string when 'fill_method' is 'constant'.")

    if "time_window" in hmv_config:
        try:
            pd.Timedelta(hmv_config["time_window"])
        except ValueError:
            log_and_raise_error("Invalid 'time_window': must be a valid pandas offset string like '1min'.")

def validate_detect_outliers(do_config):
    valid_methods = ["z_score", "iqr"]

    if "method" not in do_config or do_config["method"] not in valid_methods:
        log_and_raise_error(f"Invalid 'method' in detect_outliers: choose 'z_score' or 'iqr'.")

    if "threshold" in do_config:
        if not isinstance(do_config["threshold"], (int, float)):
            log_and_raise_error("'threshold' in detect_outliers must be a numeric value.")

def validate_check_duplicates(cd_config):
    valid_keep = ["first", "last", None]

    if "keep" in cd_config and cd_config["keep"] not in valid_keep:
        log_and_raise_error(f"Invalid 'keep' in check_duplicates: choose 'first', 'last', or None.")
