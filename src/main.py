import os
from config.config_loader import load_validate_config
from utils.logging_setup import initialize_logging, log_and_raise_error
from core.mode_runner import run_single_day, run_everyday, run_multi_days

def main():
    """
  This is the main function that reads the configuration from the config.yaml file, extracts the mode, 
  and then calls the appropriate function to run (e.g., single day static, everyday static, etc.).
  If the mode is not recognized, it logs an error and exits.  
  """
    # initialize logging
    initialize_logging(level="INFO")

    # load and validate the configuration file
    config_path = os.path.join("..", "config.yaml")
    try:
        config = load_validate_config(config_path)
    except Exception as e:
        log_and_raise_error(f"Error loading configuration: {e}")

    # get the needed var to start the plotting process
    mode = config.get("mode", "single_day")
    mode_mapping = {
        "single_day": run_single_day,
        "multi_days": run_multi_days,
        "full_data": run_everyday
    }

    # run the function based on the mode
    if mode in mode_mapping:
        mode_mapping[mode](config)
    else:
        log_and_raise_error(f"Unknown mode {mode}. Exiting...")

if __name__ == "__main__":
    main()