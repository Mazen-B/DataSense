
# DataSense
## Table of Contents
- [Usage](#usage)
- [Logging](#logging)
- [TODO](#todo)

## Usage
All necessary input parameters are defined in the `config.yaml` file. Modify this configuration file to set the log file location, processing options, mode of operation, and sensor selection. Below is an example of the `config.yaml` file and details on each section:

```yaml
logging:
  log_file: "../log_files/dummy_data.csv"
processing:
  output_dir: "../stats/"
  time_column: "time"
  time_format: "%Y-%m-%d %H:%M:%S"  # Choose from: "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S"
  date: "2024-05-31"
  mode: "single_day"  # Options: "single_day", "multi_days", "everyday", "full_data"
sensors:
  - "temp1"
  - "temp2"
  - "pressure1"
  - "pressure1"
  - "electical_power"
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
    - **everyday**: Processes data without specific dates.
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
- [ ] date_format in the config file we should be able to choose we want to one day, h, m or with s
- ### DataManger-related
  - **loaders**:
    - [ ] load excel and csv we transform the columns into lower cases before we load (in original and request)
    - [ ] make loader more robust: abstract class add dask or Fireducks for the others
  - **preprocessing**:
    - [ ] in core_preprocessing we need way more stuff if we want to work with statitics (one main method ther)
    - [ ] data quality and validation
  - **time_processing**:
    - [x] in get_filtered_data, we need to only load the needed columns for a specific time range
- ### Unit tests 
  - [ ] for conf loggic
  - [ ] for data manamgenemt loggic