
# DataSense
## Table of Contents
- [Usage](#usage)
- [Logging](#logging)
- [TODO](#todo)

## Usage
All necessary input parameters are defined in the `config.yaml` file. Modify this configuration file to set the log file location, processing options, mode of operation, and sensor selection. Below is an example of the `config.yaml` file and details on each section:

```yaml
input_file: "../input_files/missing_values.csv"
output_dir: "../output/stats/"
date: "2024-06-01" # must have this format YYYY-MM-DD
mode: "single_day" # Options: "single_day" "multi_days", "full_data"
time_column: "time"
time_format: "%Y-%m-%d %H:%M:%S"  # Choose from: "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S"
sensors:
  temperature:
    - "te306"
    - "te307"
  pressure:
    - "pe303"
    - "pe301"
  el_power:
    - "pelgross"
pre_processing:
  handle_missing_values:
    strategy: "fill"
    fill_method: "mean"
    fill_value: null
    time_window: "1min"
  detect_outliers:
    method: "z_score"
    threshold: 3
  check_duplicates:
    keep: "first"
```

### Configuration Details

- **Log File**: Path to the sensor data file (CSV or XLSX). This file must end with `.csv` or `.xlsx`, or an error will be raised.
- **Output Directory**: Specify where output statistics or plots will be stored.
- **Time Column and Format**:
  - **time_column**: The column name for timestamps in the log file.
  - **time_format**: Must match one of the accepted formats listed in the configuration. If a non-supported format is provided, an error will prompt you to choose a supported format.
- **Mode**:
  - Set the `mode` to define the data processing scope. Options:
    - **single_day**: Requires `date`.
    - **multi_days**: Requires `start_date` and `end_date`.
    
      **N.B.** When specifying both `start_date` and `end_date`, the `end_date` is exclusive. For example, if `end_date="2025-01-02"`, the data will include values up to but not including `2025-01-02 00:00:00`.
    - **full_data**: Processes the entire dataset without specific date restrictions.
- **Sensors**: A list of sensor IDs to include in the analysis (at least one is needed).

## Logging
This project includes a comprehensive logging system, configured to maintain a structured log history.

### Logging Configuration

The logging setup records information both to the console and to rotating log files, ensuring logs remain manageable over time. Logs are organized into separate directories named with the date and time of each session. 

- **Log Directory**: Logs are stored in a subdirectory under `logs` with the session timestamp. For each new session, a unique folder is created, containing a `run.log` file.
- **Log Rotation**: Each log file is limited to 5MB by default, with up to 5 backups maintained to prevent excessive log accumulation.
- **Error Handling**: Errors are logged and can be raised as exceptions for debugging.

## TODO
- ### Config-related
- [x] every input in the config should be validates (only csv or excel), etc...
- [x] more specific config with sections: processing, preprosessing and so on where we add all the needed inputs
- ### DataManger-related
  - **loaders**:
    - [x] have the full data option
    - [ ] load excel and csv we transform the columns into lower cases before we load (in original and request) - call the validation function from the the DataChecker class 
    - [ ] make loader more robust: abstract class add dask or Fireducks for the others
  - **preprocessing**:
    - [x] in core_preprocessing we need way more stuff if we want to work with statitics (one main method there)
    - [x] add a section in the conf file for the prepresessing options
    - [x] in case of onecoding/categorical cols, add a logic that handle missing values
    - [ ] flag the outlier or remove them (with None option)
    - [x] more comprehensive log msgs
    - [x] drop is returning an error in the time column even removing duplications
  - **time_processing**:
    - [x] in get_filtered_data, we need to only load the needed columns for a specific time range
- ### Unit tests 
  - [ ] for conf logic
  - [x] for data processing logic, especially the values that i am replacing
  - [x] add a script that automate the execution of the tests

Example: Rule: If sensor1 > threshold and sensor10 < threshold → error_signal = True
