from src.util import get_csv_header, get_parquet_header
from src.dq_checks.check_result import CheckResult
from collections import Counter
from src.config import LOGGER
from src.data_model import DataModel

def check_duplicated_column_in_csv(file_path: str, table_name: str, duckdb_conn = None, context = None) -> CheckResult:
    """
    Check if the CSV file has duplicated columns in its header.

    Parameters:
        file_path (str): Path to the CSV file.
        table_name (str): Name of the CDM table to check against.
        duckdb_conn: Optional DuckDB connection for logging.
        context: Optional context object for additional runtime information update.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header has duplicated columns.
    """
    csv_header = get_csv_header(file_path, duckdb_conn=duckdb_conn)
    if len(csv_header) > len(set(csv_header)):
        duplicated_columns = [item for item, count in Counter(csv_header).items() if count > 1]
        result = CheckResult(
            check_type = 'csv header duplication',
            status = 'FAIL',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(duplicated_columns),
            troubleshooting_message = 'The file will not be loaded. Please remove duplicated column(s) in file. '
        )
        if context:
            # if the csv has duplicated columns, don't load the table to duckdb. Also skip other checks on this table.
            context.skip_duckdb_load_tables.append(table_name)
            context.skip_check_tables.append(table_name)
    else:
        result = CheckResult(
            check_type = 'csv header duplication',
            status = 'PASS',
            file_name=file_path,
            table_name = table_name,
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_extra_column_in_csv(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None) -> CheckResult:
    """
    Check if the CSV file has extra columns that are not defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the CSV file.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.
        duckdb_conn: Optional DuckDB connection for logging.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header has extra columns.
    """
    csv_header = get_csv_header(file_path, duckdb_conn=duckdb_conn)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in csv
    extra_csv_column = set(csv_header) - set(cdm_columns)
    if len(extra_csv_column) > 0:
        result = CheckResult(
            check_type = 'extra column in csv header',
            status = 'WARN',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(extra_csv_column)
        )
    else:
        result = CheckResult(
            check_type = 'extra column in csv header',
            status = 'PASS',
            table_name = table_name
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_missing_column_in_csv(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None, context = None) -> CheckResult:
    """
    Check if the CSV file has all the required columns defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the CSV file.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.
        duckdb_conn: Optional DuckDB connection for logging.
        context: Optional context object for additional runtime information update.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header is missing any required columns.
    """
    csv_header = get_csv_header(file_path, duckdb_conn=duckdb_conn)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in csv
    missing_csv_column = set(cdm_columns) - set(csv_header)
    if len(missing_csv_column) > 0:
        result = CheckResult(
            check_type = 'missing column in csv header',
            status = 'FAIL',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(missing_csv_column)
        )
        if context:
            context.skip_check_columns[table_name] = context.skip_check_columns.get(table_name, tuple()) + tuple(missing_csv_column)
    else:
        result = CheckResult(
            check_type = 'missing column in csv header',
            status = 'PASS',
            table_name = table_name,
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_extra_column_in_parquet(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None) -> CheckResult:
    """
    Check if the Parquet file has extra columns that are not defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the Parquet file or folder containing parquet files.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.
        duckdb_conn: Optional DuckDB connection for logging.

    Returns:
        CheckResult: Result of the check, indicating whether the Parquet header has extra columns.
    """
    parquet_header = get_parquet_header(file_path, duckdb_conn=duckdb_conn)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in parquet
    extra_parquet_column = set(parquet_header) - set(cdm_columns)
    if len(extra_parquet_column) > 0:
        result = CheckResult(
            check_type = 'extra column in parquet header',
            status = 'WARN',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(extra_parquet_column)
        )
    else:
        result = CheckResult(
            check_type = 'extra column in parquet header',
            status = 'PASS',
            table_name = table_name
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_missing_column_in_parquet(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None, context = None) -> CheckResult:
    """
    Check if the Parquet file has all the required columns defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the Parquet file or folder containing parquet files.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.
        duckdb_conn: Optional DuckDB connection for logging.
        context: Optional context object for additional runtime information update.

    Returns:
        CheckResult: Result of the check, indicating whether the Parquet header is missing any required columns.
    """
    parquet_header = get_parquet_header(file_path, duckdb_conn=duckdb_conn)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in parquet
    missing_parquet_column = set(cdm_columns) - set(parquet_header)
    if len(missing_parquet_column) > 0:
        result = CheckResult(
            check_type = 'missing column in parquet header',
            status = 'FAIL',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(missing_parquet_column)
        )
        if context:
            context.skip_check_columns[table_name] = context.skip_check_columns.get(table_name, tuple()) + tuple(missing_parquet_column)
    else:
        result = CheckResult(
            check_type = 'missing column in parquet header',
            status = 'PASS',
            table_name = table_name,
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result