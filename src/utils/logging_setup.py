import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from utils.file_management import create_output_dir, cleanup_old_logs

def initialize_logging(level="INFO", log_dir_name="system_logs", max_bytes=5*1024*1024, backup_count=5):
    """
  This function initializes logging with the specified level, console and file logging, and log rotation.
  """
    # create the logs dir if it does not exist
    log_dir = os.path.join("..", log_dir_name)
    create_output_dir(log_dir)

    # clean up old log directories if there are more than 5
    cleanup_old_logs(log_dir, max_logs=4)

    # remove any existing handlers to avoid duplicate logs
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # create a unique directory for each logging session, named with date and time
    session_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_path = os.path.join(log_dir, session_time)
    os.makedirs(log_path, exist_ok=True)

    log_file = os.path.join(log_path, "run.log")

    # set up basic configuration for logging to console
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            # Logs to console
            logging.StreamHandler(),  
            # Logs to file with rotation
            RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        ]
    )
    
    logging.info("Logging initialized. Logs are being saved to %s", log_file)

def log_and_raise_error(message):
    """
  This function logs an error message and raises a ValueError with the same message.
  """
    logging.error(message)
    raise ValueError(message)

def log_and_raise_exception(message):
    """
  This function logs an exception message and raises a Exception with the same message.
  """
    logging.error(message)
    raise Exception(message)
