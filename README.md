# infomodels-duckdb

A Python package for running data quality checks on any Common Data Model.  
It provides utilities to validate data integrity rules, such as constraints and relationships, with structured logging and result summaries. The package is designed to be extensible, allowing additional data quality tests to be added as needed.

## Quick Start

### Docker

1. **Clone the repository:**

    ```bash
    git clone https://github.com/PEDSnet/infomodels-duckdb.git
    cd infomodels-duckdb
    ```

2. **Edit the configuration:**

    Copy the template configuration file and update it as needed:

    ```bash
    cp config.yml.docker_template config.yml
    ```

    Then, edit `config.yml` to match your submission and site file format.

3. **Build the Docker image:**

    ```bash
    docker build -t infomodels-duckdb .
    ```
4. **Run the main script in a container:**

    ```bash
    docker run --rm -it \
      -v PATH_TO_YOUR_CDM_DIR:/data \
      -v PATH_TO_YOUR_RESULT_DIR:/result \
      infomodels-duckdb
    ```

### Standalone

1. **Clone the repository:**

    ```bash
    git clone https://github.com/PEDSnet/infomodels-duckdb.git
    cd infomodels-duckdb
    ```
2. **Activate virtual environment and install dependencies (optional, but recommended):**  
   This isolates your Python environment for the project. Alternatively, you may install the package and its dependencies system-wide if preferred.

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3. **Edit the configuration:**

    Copy the template configuration file and update it as needed:

    ```bash
    cp config.yml.standalone_template config.yml
    ```

    Then, edit `config.yml` to match your submission and site file format.


4. **Run the main script:**

    ```bash
    python -m src.main
    ```

## Running From AWS S3

You can run validation against CSV or Parquet files stored in S3 by setting these options in `config.yml`:

```yaml
submission_files:
    storage_type: s3
    BUCKET_NAME: your-bucket
    BUCKET_PATH: path/inside/bucket
    S3_KEY: YOUR_AWS_ACCESS_KEY_ID
    S3_SECRET: YOUR_AWS_SECRET_ACCESS_KEY
    file_format: csv        # or parquet
    multiple_file_per_table: false
```

Notes:
- `BUCKET_PATH` is optional; leave empty to use bucket root.
- `dir` is ignored when `storage_type: s3`.
- For local mode, keep `storage_type: local` and use `submission_files.dir`.

## Implemented Checks

The following data quality checks are currently supported:
- **Missing Submission File:** Detects required files that are missing from the submission.
- **Extra Submission File:** Detects unexpected files present in the submission.
- **Duplicated Column in CSV:** Identifies duplicate column names in CSV headers.
- **Extra Column in CSV:** Flags columns in CSV files that are not defined in the data model.
- **Missing Column in CSV:** Flags columns defined in the data model that are missing from the CSV file.
- **Data Type:** The data types in the CSV files conform to the column definitions specified in the CDM.
- **NOT NULL Violation:** Ensures specified columns do not contain NULL values.
- **Distinct Violation:** Ensures specified columns (or combinations) contain only unique values.
- **Primary Key Violation:** Checks that primary key columns are both NOT NULL and unique.
- **Foreign Key Violation:** Checks that values in a main table reference valid values in a related table.

More checks will be added. 
