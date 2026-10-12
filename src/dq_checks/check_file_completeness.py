from __future__ import annotations
import os
from typing import Tuple, List, Set
from src.dq_checks.check_result import CheckResult
from src.config import LOGGER
from src.constants import OPTIONAL_TABLES

def _file_extension_for(file_format: str) -> str:
    """Map a file_format to the extension a single-file-per-table submission uses."""
    if file_format == 'csv':
        return '.csv'
    if file_format == 'parquet':
        return '.parquet'
    raise ValueError(f"Unsupported file_format: {file_format}. Supported types are 'csv' and 'parquet'.")


def _submission_name_for_table(table_name: str, file_format: str, multiple_file_per_table: bool) -> str:
    """
    The name a table has on disk: '{table}.csv' / '{table}.parquet' when each table is one
    file, or the bare directory name when a table is a folder of parts.
    """
    if multiple_file_per_table:
        return table_name
    return table_name + _file_extension_for(file_format)


def _get_table_names_from_files(
        file_dir: str,
        file_format: str,
        multiple_file_per_table: bool
    ) -> Set[str]:
    """
    Helper function to get table names from files in the directory.
    Parameters:
        file_dir (str): Path to the directory containing submission files.
        file_format (str): file format, either 'csv' or 'parquet'.
        multiple_file_per_table (bool): whether each table can have multiple files. If True, will check if there's a folder named after the table. If False, will check for single file named {table_name}.[csv/parquet] depending on file type.
    Returns:
        Set[str]: Set of table names derived from the files in the directory.
    """
    table_names = set()
    if not multiple_file_per_table:
        file_extension = _file_extension_for(file_format)
        table_names = {os.path.splitext(f)[0] for f in os.listdir(file_dir) if f.endswith(file_extension)}
    else:
        dir_names_on_dir = [d for d in os.listdir(file_dir) if os.path.isdir(os.path.join(file_dir, d))]
        table_names = set(dir_names_on_dir)
    LOGGER.info(f"Table names found from files: {table_names}")
    return table_names

def check_missing_submission_file(
        file_dir: str, 
        cdm_tables_expected: Tuple[str, ...],
        file_format: str = 'csv',
        multiple_file_per_table: bool = False,
        duckdb_conn = None
    ) -> CheckResult:
    """
    Check if the directory misses any expected CSV files for each CDM table.

    Parameters:
        file_dir (str): Path to the directory containing submission files.
        cdm_tables_expected (Tuple[str, ...]): Tuple of expected CDM table names (without file extension).
        file_format (str): file format, either 'csv' or 'parquet'. Default to 'csv'.
        multiple_file_per_table (bool): whether each table can have multiple files. If True, will check if there's a folder named after the table. If False, will check for single file named {table_name}.[csv/parquet] depending on file type. Default to False.
    Returns:
        CheckResult
    """
    LOGGER.info("Running DQ check: check_missing_submission_file. "
            f"Params: file_dir={file_dir}, cdm_tables_expected={cdm_tables_expected}, file_format={file_format}, multiple_file_per_table={multiple_file_per_table}")
    check_type = 'missing_submission_file'
    if cdm_tables_expected is None or len(cdm_tables_expected) == 0:
        result = CheckResult(
            check_type = 'missing_submission_file',
            status = 'SKIPPED',
            troubleshooting_message = 'No expected CDM tables provided to check for missing submission files.'
        )
        result.log(LOGGER, duckdb_conn=duckdb_conn)
        return(result)    
    table_names_from_files = _get_table_names_from_files(
        file_dir = file_dir,
        file_format = file_format,
        multiple_file_per_table = multiple_file_per_table
    )
    missing_tables = set(cdm_tables_expected) - set(table_names_from_files) - set(OPTIONAL_TABLES)
    if len(missing_tables) > 0:
        result = CheckResult(
            check_type = check_type,
            status = 'FAIL',
            table_name = tuple(missing_tables),
            troubleshooting_message = f'Cannot find submission file(s) for above table(s) in dir: {file_dir}'
        )
    else:
        result = CheckResult(
            check_type = check_type,
            status = 'PASS'
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return(result)

def check_extra_submission_file(
        file_dir: str, 
        cdm_tables_expected: Tuple[str, ...],
        file_format: str = 'csv',
        multiple_file_per_table: bool = False,
        duckdb_conn = None
    ) -> CheckResult:
    """
    Check if the directory contains submission files for tables the data model does not define.

    Such a file is never loaded or submitted, so it usually means a misnamed table or a
    config pointed at the wrong directory. Reported as WARN rather than FAIL, with the names
    the files have on disk ('{table}.csv', or the folder name when multiple_file_per_table).

    Parameters:
        file_dir (str): Path to the directory containing submission files.
        cdm_tables_expected (Tuple[str, ...]): Tuple of expected CDM table names (without file extension).
        file_format (str): file format, either 'csv' or 'parquet'. Default to 'csv'.
        multiple_file_per_table (bool): whether each table can have multiple files. If True, will check if there's a folder named after the table. If False, will check for single file named {table_name}.[csv/parquet] depending on file type. Default to False.

    Returns:
        CheckResult
    """
    LOGGER.info("Running DQ check: check_extra_submission_file. "
                f"Params: file_dir={file_dir}, cdm_tables_expected={cdm_tables_expected}, file_format={file_format}, multiple_file_per_table={multiple_file_per_table}")
    check_type = 'extra_submission_file'
    if cdm_tables_expected is None or len(cdm_tables_expected) == 0:
        result = CheckResult(
            check_type = 'extra_submission_file',
            status = 'SKIPPED',
            troubleshooting_message = 'No expected CDM tables provided to check for extra submission files.'
        )
        result.log(LOGGER, duckdb_conn=duckdb_conn)
        return(result)    
    table_names_from_files = _get_table_names_from_files(
        file_dir = file_dir,
        file_format = file_format,
        multiple_file_per_table = multiple_file_per_table
    )
    # on disk but not in the data model; the mirror image of check_missing_submission_file
    extra_tables = set(table_names_from_files) - set(cdm_tables_expected)
    if len(extra_tables) > 0:
        extra_files = tuple(sorted(
            _submission_name_for_table(table, file_format, multiple_file_per_table)
            for table in extra_tables
        ))
        result = CheckResult(
            check_type = check_type,
            status = 'WARN',
            file_name = extra_files,
            troubleshooting_message = "Extra file(s) in submission directory. These files will not be loaded and submitted. Please make sure there's no config issue."
        )
    else:
        result = CheckResult(
            check_type = check_type,
            status = 'PASS'
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return(result)
