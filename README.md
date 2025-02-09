
# DataSense
## 📌 Table of Contents
- [Overview](#overview)
- [Installation & Usage](#⚙️-installation--usage)
- [Configuration](#🛠️-configuration)
- [Output Files](#📂-output-files)
- [Logging](#📝-logging)
- [License](#📜-license)


## Overview
DataSense is a **dataset processing and association rule mining tool** designed to clean and prepare structured data for analysis. It supports CSV/Excel files, allowing users to process entire datasets or filter specific time ranges efficiently. The processed data can then be used for **Exploratory Data Analysis (EDA)**, **Machine Learning (ML)**, or **Rule Mining**. In this case, **Rule Mining** is implemented, with an additional preprocessing script to handle the required transformations.


## ⚙️ Installation & Usage
### Prerequisites
Ensure the following libraries are installed:
```bash
pip install pyyaml pandas mlxtend
```
❗ **Note:** For `mlxtend` compatibility,  use any version higher than `0.18`, but avoid `0.23.2` and `0.23.3` due to a potential bug. I recommend using version `0.23.1`.

### Running DataSense
Navigate to the `src` directory and execute:
```bash
cd src
python main.py
```

## 🛠️ Configuration
All necessary input parameters are defined in the `config.yaml` file. Modify this configuration file to set the log file location, processing options, mode of operation, and sensor selection. Below are some key configuration details:
- **input_file**: Must end with `.csv` or `.xlsx`.
- **output_dir**: Specify a valid, non-empty directory path.
- **Time Configuration**:
  - **time_column**: Non-empty string.
  - **time_format**: One of: `%Y-%m-%d %H:%M:%S`, `%d/%m/%Y %H:%M:%S`, `%m-%d-%Y %H:%M:%S`.
- **Mode**: Automatically determined based on provided dates (for all dates please use this format: "YYYY-MM-DD"):
  - **single_day**: Requires `date` (e.g., "2024-06-01")
  - **time_range**: Requires both `start_date` and `end_date` (exclusive).
  - **full_data**: No date needed. If no date is specified, the default mode is `full_data`.
- **Sensors**: Specify at least one sensor division:
  - Allowed divisions: `temperature`, `pressure`, `el_power`, `rpm`, `ordinal`, `categorical`.
- **Pre-Processing**:
  - **handle_missing_values**: Strategy (`drop`, `fill`) with optional `fill_method` (`ffill`, `bfill`, `mean`, `median`, `mode`, `constant`, `interpolate`).
  - **detect_outliers**: Method (`z_score`, `iqr`) with a `threshold` (numeric).
  - **time_col**: Options:
    - `check_duplicates_keep`: `first`, `last`, or `None`.
    - `handle_missing_values`: `error`, `drop`.
    - `failed_datetime_conversion`: `error`, `drop`.
  - **rule_mining**:
    - Method (`equal_width`, `quantile`), `bins` (positive integer), and optional `labels` (list or string).
    - **continuous_sensor_types**: Non-empty list of strings.
    - Needed thresholds: `min_support`, `min_confidence`, `min_lift` (positive floats).

##  📂 Output Files
After running the tool, the following output files are generated and stored in the directory specified in the output_dir parameter of the config file:

    1️⃣ processed_data.csv → Cleaned dataset, filtered based on the specified columns and time range (if provided in the config file). 
    2️⃣ processed_data_mining_rules.csv → Dataset prepared for rule mining.
    3️⃣ generated_rules.txt → Extracted association rules.

## 📝 Logging
Logs are stored in `system_logs/{session_timestamp}/run.log` with **rotating log files** (5MB max per file, up to 5 backups). Errors are logged and can be raised as exceptions.

## 📜 License
Licensed under the **MIT License**, allowing free use and modification.
