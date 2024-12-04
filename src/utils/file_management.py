import os
import shutil
import logging

def create_output_dir(output_dir):
    """
  This function ensures that the output directory exists. If it does not exist, it creates it.
  """
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Created output directory: {output_dir}")

def cleanup_output_dir(output_dir):
    """
  This function deletes the output directory if it exists.
  """
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        logging.info(f"Output directory '{output_dir}' has been deleted.")

def cleanup_old_logs(log_dir, max_logs=5):
    """
  This function keeps only the latest "max_logs" directories in "log_dir".
  If there are more than "max_logs" directories, it deletes the oldest ones.
  """
    # get a sorted list of directories by creation time
    all_logs = sorted(
        [os.path.join(log_dir, d) for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d))],
        key=os.path.getctime
    )

    # delete oldest directories if the count exceeds "max_logs"
    if len(all_logs) > max_logs:
        old_logs = all_logs[:len(all_logs) - max_logs]
        for old_log in old_logs:
            shutil.rmtree(old_log)
            logging.info(f"Old log directory '{old_log}' has been deleted.")
