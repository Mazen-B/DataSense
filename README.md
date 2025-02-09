
# DataSense
## Table of Contents
- [Overview](#overview)
- [Configuration Setup and Usage](#configuration-setup-and-usage)
- [Logging](#logging)
- [Workflow Overview](#workflow-overview)
- [Run the Tool](#run-the-tool)
- [License](#license)

## Overview
DataSense is a project focused on **data processing and cleaning**, ensuring the dataset is ready for further analysis. It allows users to process CSV or Excel files by loading the **full dataset** or a specific **time range/single day** (for efficiency). The processed data can then be used for **Exploratory Data Analysis (EDA)**, **Machine Learning (ML)**, or **Rule Mining**. In this case, **Rule Mining** is implemented, with an additional preprocessing script to handle the required transformations.

## Configuration Setup and Usage
All necessary input parameters are defined in the `config.yaml` file. Modify this configuration file to set the log file location, processing options, mode of operation, and sensor selection. Below is an example of the `config.yaml` file and details on each section:

```yaml
input_file: "../input_files/dummy_data.csv"
output_dir: "../output/"
time_column: "time"
time_format: "%Y-%m-%d %H:%M:%S"
# Specify date (e.g., "2024-06-01") for a single-day analysis
# Use start_date and end_date for a time range
# If no date is specified, the full dataset will be loaded
sensors:
  temperature:
    - "sensor1"
    - "sensor2"
  pressure:
    - "sensor3"
  el_power:
    - "sensor4"
  rpm:
    - "sensor5"
    - "sensor6"
  ordinal:
    - "sensor7"
  categorical:
    - "sensor8"
pre_processing:
  time_col:
    check_duplicates_keep: "first"
    handle_missing_values: "drop"
    failed_datetime_conversion: "error"
  handle_missing_values:
    strategy: "fill"
    fill_method: "mean"
    fill_value: null
    time_window: "1min"
  detect_outliers:
    method: "z_score"
    threshold: 3
  rule_mining:
    method: "equal_width"
    bins: 3
    labels: null
    continuous_sensor_types:
      - "temperature"
      - "pressure"
      - "el_power"
      - "rpm"
    min_support: 0.1
    min_confidence: 0.7
    min_lift: 1.0
```
### Configuration Details

- **Input File**: Must be end with `.csv` or `.xlsx`.
- **Output Directory**: Specify a valid, non-empty directory path.
- **Time Configuration**:
  - **time_column**: Non-empty string.
  - **time_format**: One of: `%Y-%m-%d %H:%M:%S`, `%d/%m/%Y %H:%M:%S`, `%m-%d-%Y %H:%M:%S`.
- **Mode**: Automatically determined based on provided dates:
  - **single_day**: Requires `date`.
  - **time_range**: Requires both `start_date` and `end_date` (exclusive).
  - **full_data**: No date restrictions.
- **Sensors**: Specify at least one sensor division:
  - Allowed divisions: `temperature`, `pressure`, `el_power`, `rpm`, `ordinal`, `categorical`.
- **Pre-Processing**:
  - **handle_missing_values**: Strategy (`drop`, `fill`) with optional `fill_method` (`ffill`, `bfill`, `mean`, `median`, `mode`, `constant`, `interpolate`).
  - **detect_outliers**: Method (`z_score`, `iqr`) and optional `threshold` (numeric).
  - **time_col**: Options:
    - `check_duplicates_keep`: `first`, `last`, or `None`.
    - `handle_missing_values`: `error`, `drop`.
    - `failed_datetime_conversion`: `error`, `drop`.
  - **rule_mining**:
    - Method (`equal_width`, `quantile`), `bins` (positive integer), and optional `labels` (list or string).
    - **continuous_sensor_types**: Non-empty list of strings.
    - Optional thresholds: `min_support`, `min_confidence`, `min_lift` (positive floats).

##  Output Files

After running the tool, the following output files are generated and stored in the directory specified in the output_dir parameter of the config file:

1. **processed_data.csv** - The cleaned dataset, filtered based on the specified columns and time range (if provided in the config file).

2. **processed_data_mining_rules.csv** - A further processed dataset, prepared specifically for rule mining.

3. **generated_rules.txt** - A text file containing all generated rules from the rule mining process.

## Logging
This project includes a comprehensive logging system, configured to maintain a structured log history.

### Logging Configuration

The logging setup records information both to the console and to rotating log files, ensuring logs remain manageable over time. Logs are organized into separate directories named with the date and time of each session. 

- **Log Directory**: Logs are stored in a subdirectory under `system_logs` with the session timestamp. For each new session, a unique folder is created, containing a `run.log` file.
- **Log Rotation**: Each log file is limited to 5MB by default, with up to 5 backups maintained to prevent excessive log accumulation.
- **Error Handling**: Errors are logged and can be raised as exceptions for debugging.

## Workflow Overview
The DataSense tool operates in several key stages:

### 1. Configuration Loading and Validation

- The tool starts by loading the `config.yaml` file using the `config_loader.py` module.
- The configuration is validated by checking the presence of required keys and ensuring proper formatting of parameters such as time range and sensor types (`validate_config.py`).

### 2. Data Loading

- The `data_loader.py` dynamically determines whether the input file is in CSV or Excel format and uses appropriate readers.
- Depending on the selected mode (`full`, `single_day`, or `time_range`), either the entire dataset or a specific time slice is loaded.

### 3. Time Column Processing

The `time_preprocessor.py` handles time column conversion, sorting, duplicate removal, and missing value handling based on configuration settings. The main steps involved are:

- `validate_time_column()`: Ensures that the time column exists and contains valid data.
- `handle_missing_values(method)`: Handles missing values using either the `drop` or `error` method.
- `convert_to_datetime(errors="coerce")`: Converts the time column to datetime format.
- `handle_failed_datetime_conversion(action)`: Deals with rows where datetime conversion failed by either dropping them or raising an error.
- `order_time_column()`: Orders the DataFrame by the time column.
- `check_duplicates(keep)`: Checks for and removes duplicate rows based on the time column. Options for `keep` include:
  - `"first"`: Keeps the first occurrence of each duplicate.
  - `"last"`: Keeps the last occurrence of each duplicate.
  - `None`: Logs a warning if duplicates exist without removing them.

### 4. Data Cleaning and Preprocessing

The `core_preprocessor.py` module handles data cleaning and validation. Key steps include:

- `validate_columns()`: Ensures that all required columns are present and non-empty.
- `standardize_column_names()`: Standardizes column names by converting them to lowercase and replacing spaces with underscores.
- `handle_missing_values(strategy, fill_method, fill_value, time_window)`: Handles missing values according to the specified strategy (`drop` or `fill`) and fill method (`mean`, `median`, etc.).
- `encode_categorical_and_booleans()`: Encodes categorical and boolean columns into numerical representations.
- `validate_data_types()`: Ensures that columns have the expected data types.
- `detect_outliers(method, threshold)`: Detects outliers in sensor columns using methods like Z-score or IQR.
- `last_emptness_check()`: Checks for any remaining empty values and raises an error if any are found.

### 5. Rule Mining Preprocessing

The `rule_mining_processor.py` module performs advanced preprocessing for rule mining. It includes:

- `discretize_and_encode(continuous_sensor_types, method, bins, labels)`: Discretizes continuous sensor data into bins using either the `equal_width` or `quantile` method and one-hot encodes the bins.
- `clean_and_encode_ordinal(ordinal_sensor_types)`: Removes the time column and one-hot encodes ordinal columns.
- `convert_categorical_to_bool(categorical_sensors)`: Converts categorical columns to boolean type.
- `last_emptness_check()`: Ensures that all columns are not empty and raises an error if non-binary values are found.

### 6. Association Rule Mining (running FP-Growth algorithm)
- Extracts rules using FP-Growth based on minimum support, confidence, and lift criteria.

## Run the Tool
Ensure the following modules are installed before running the tool:

- **Required Python Libraries**:
  - `pyyaml`: For loading and parsing configuration files.
  - `pandas`: For data manipulation and analysis.
  - `mlxtend`: For implementing FP-Growth and generating association rules.
  - `unittest`: For unit testing.
  - `pathlib`: For working with file paths.
  - `logging`: For logging events during execution.

To install the required dependencies, run:

```bash
pip install pyyaml pandas mlxtend
```

To run the tool, navigate to the src directory and execute the following command:
```bash
cd src
python main.py
```
### ⚠️ Note on `mlxtend` Compatibility

There is a potential bug in `mlxtend` versions **0.23.2 and 0.23.3**, where the `association_rules()` function unexpectedly requires a `num_itemsets` argument. This issue does not exist in version **0.23.1** and earlier.

#### Recommended Solution:
I **Install `mlxtend` 0.23.1** or a version higher than 0.23.3 (if available).

#### ❌ If you encounter this error:
TypeError: association_rules() missing 1 required positional argument: 'num_itemsets'
#### ✅ Fix Options:
1. **Downgrade `mlxtend` to 0.23.1** (Recommended):
    ```bash
    pip install mlxtend==0.23.1
    ```
2. Manually specify num_itemsets in the function call:
    ```python
    rules = association_rules(frequent_itemsets, num_itemsets=len(discretized_data), metric="confidence", min_threshold=min_confidence)
    ```

## License
This project is licensed under the MIT License, allowing free use and modification.