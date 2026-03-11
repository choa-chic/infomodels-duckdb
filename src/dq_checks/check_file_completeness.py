from __future__ import annotations
import os
from typing import Tuple, List, Set
from src.dq_checks.check_result import CheckResult
from src.config import LOGGER
from src.constants import OPTIONAL_TABLES

def _get_table_names_from_files(
        file_dir: str,
        file_format: str,
    multiple_file_per_table: bool,
    storage_type: str = 'local',
    duckdb_conn = None,
    bucket_name: str = None,
    bucket_path: str = None,
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
    if storage_type == 's3':
        if duckdb_conn is None:
            raise ValueError("duckdb_conn is required when storage_type='s3'.")
        if not bucket_name:
            raise ValueError("bucket_name is required when storage_type='s3'.")
        bucket_path_norm = (bucket_path or '').strip('/')
        s3_base = f"s3://{bucket_name}"
        if bucket_path_norm:
            s3_base = f"{s3_base}/{bucket_path_norm}"
        if file_format == 'csv':
            file_extension = '.csv'
        elif file_format == 'parquet':
            file_extension = '.parquet'
        else:
            raise ValueError(f"Unsupported file_format: {file_format}. Supported types are 'csv' and 'parquet'.")

        if not multiple_file_per_table:
            pattern = f"{s3_base}/*{file_extension}"
            files = [item[0] for item in duckdb_conn.execute("SELECT file FROM glob(?);", (pattern,)).fetchall()]
            table_names = {os.path.splitext(os.path.basename(f))[0] for f in files}
        else:
            pattern = f"{s3_base}/*/*{file_extension}"
            files = [item[0] for item in duckdb_conn.execute("SELECT file FROM glob(?);", (pattern,)).fetchall()]
            table_names = {os.path.basename(os.path.dirname(f)) for f in files}
        LOGGER.info(f"Table names found from S3 files: {table_names}")
        return table_names

    if not multiple_file_per_table:
        if file_format == 'csv':
            file_extension = '.csv'
        elif file_format == 'parquet':
            file_extension = '.parquet'
        else:
            raise ValueError(f"Unsupported file_format: {file_format}. Supported types are 'csv' and 'parquet'.")
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
    storage_type: str = 'local',
    bucket_name: str = None,
    bucket_path: str = None,
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
        multiple_file_per_table = multiple_file_per_table,
        storage_type = storage_type,
        duckdb_conn = duckdb_conn,
        bucket_name = bucket_name,
        bucket_path = bucket_path,
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
    storage_type: str = 'local',
    bucket_name: str = None,
    bucket_path: str = None,
        duckdb_conn = None
    ) -> CheckResult:
    """
    Check if the directory contains extra CSV files for each CDM table.

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
    filenames_from_tables = _get_table_names_from_files(
        file_dir = file_dir,
        file_format = file_format,
        multiple_file_per_table = multiple_file_per_table,
        storage_type = storage_type,
        duckdb_conn = duckdb_conn,
        bucket_name = bucket_name,
        bucket_path = bucket_path,
    )
    extra_files = set(filenames_from_tables) - set(filenames_from_tables)
    if len(extra_files) > 0:
        result = CheckResult(
            check_type = check_type,
            status = 'WARN',
            file_name = tuple(extra_files),
            troubleshooting_message = "Extra file(s) in directory. These files will not be loaded and submitted. Pleased make sure there's no config issue."
        )
    else:
        result = CheckResult(
            check_type = check_type,
            status = 'PASS'
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return(result)
