import os
from core.mode_runner import run_analysis
from config.config_loader import load_validate_config
from utils.logging_setup import initialize_logging, log_and_raise_error

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
    mode = config.get("mode")
    mode_mapping = {
        "single_day": lambda: run_analysis(config, "single_day"),
        "time_range": lambda: run_analysis(config, "time_range"),
        "full_data": lambda: run_analysis(config, "full_data")
    }

    # run the function based on the mode
    if mode in mode_mapping:
        mode_mapping[mode]()
    else:
        log_and_raise_error(f"Unknown mode '{mode}'. Available modes: {', '.join(mode_mapping.keys())}.")

if __name__ == "__main__":
    main()