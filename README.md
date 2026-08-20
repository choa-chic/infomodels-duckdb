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
   You only have to install the packages once but must activate the virtual environment before every run.

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
    **Windows PowerShell**
    ```bash
    python3 -m venv .venv
    .\venv\Scripts\Activate.ps1
    python3 pip install -r requirements.txt
    ```

4. **Edit the configuration:**

    Copy the template configuration file and update it as needed:

    ```bash
    cp config.yml.standalone_template config.yml
    ```

    Then, edit `config.yml` to match your submission and site file format.


5. **Run the main script:**

    ```bash
    python -m src.main
    ```
    **Windows PowerShell**
   ```bash
   python3 -m src.main
   ```

## Access Modes

`submission_files.access_mode` controls whether the submission data is copied into the DuckDB file or read in place.

- `copy` (default) loads each table's rows into a DuckDB table. This is the original behaviour.
- `pointer` creates a DuckDB view over the parquet files instead. The rows stay in the parquet, so the run does not write a second copy of the data to disk. Currently implemented for `file_format: parquet` only.

```yaml
submission_files:
    dir: /PATH/TO/DIR/WITH/PARQUET/FILES
    file_format: parquet
    access_mode: pointer
```

Pointer views are shaped like the tables `copy` mode would have built: column names are lower-cased and cast to the data model's declared types, columns the data model defines but the file omits are exposed as typed NULLs, and columns present in the file but absent from the data model are exposed as VARCHAR. Every check therefore behaves the same in either mode.

Notes:
- When a table has several parquet parts, they are combined with `union_by_name`, so parts written with differing column orders still line up by column name.
- Because the rows are not copied, the parquet files must stay readable for the whole run.
- A value that does not fit its declared type raises when the check that touches it runs, rather than when the file is loaded.

### Subsuming the submission files into the DuckDB file

`submission_files.materialize` turns the pointer views into real tables once every check has run, so the DuckDB file can be submitted on its own with all of the data inside it. It only applies when `access_mode: pointer`.

- `off` (default) leaves the pointer views as views. The DuckDB file is only usable while the parquet files are still in place.
- `keep` copies each view's rows into a table and leaves the parquet files alone.
- `consume` copies each view's rows into a table and then deletes that table's submission file. This is what keeps peak disk below `copy` mode's: the parquet files go away one at a time as the DuckDB file grows, instead of both existing in full at once.

```yaml
submission_files:
    dir: /PATH/TO/DIR/WITH/PARQUET/FILES
    file_format: parquet
    access_mode: pointer
    materialize: consume
```

A materialized table is identical to the one `copy` mode would have built -- same columns, in the data model's order, with the same types -- so the resulting file does not depend on which mode produced it.

Notes:
- Materializing happens after every check, never during. The views are live handles on the parquet, so consuming a file early would pull it out from under a check that still needs it.
- Each table is built and its row count verified before its view is dropped, so a failure part way through leaves the view in place.
- `consume` deletes submission files permanently. If the run recorded any DQ failure it declines to delete and logs a warning, since the files are what you need to diagnose the failure. Only run it against an export you can regenerate.
- `materialize` is rejected with `access_mode: copy`, where the rows are already in the file.

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
- **Check Fact Relationship:** Validates all fact_id values in fact_relationship exist in the corresponding fact tables.

More checks will be added. 
