from __future__ import annotations
import os
from typing import Tuple, List, Set
from src.dq_checks.check_result import CheckResult
from src.config import LOGGER
from src.constants import OPTIONAL_TABLES


def _extract_table_prefix_from_filename(filename: str, file_format: str) -> str:
    return filename.split('.', 1)[0]


def _allowed_suffixes(file_format: str) -> tuple[str, ...]:
    if file_format == 'csv':
        return ('.csv',)
    if file_format in ('parquet', 'pq'):
        return ('.parquet', '.pq')
    raise ValueError(f"Unsupported file_format: {file_format}. Supported types are 'csv', 'parquet', 'pq'.")


def _extract_table_name_from_path(file_path: str, file_format: str) -> str:
    table_name = _extract_table_prefix_from_filename(os.path.basename(file_path), file_format)
    if table_name.startswith(f'.{file_format}_') or table_name == '':
        parent_name = os.path.basename(os.path.dirname(file_path))
        table_name = _extract_table_prefix_from_filename(parent_name, file_format)
    return table_name


def _canonicalize_table_name(candidate_name: str, expected_tables_lower: set[str]) -> str:
    candidate_lower = candidate_name.lower()
    if candidate_lower in expected_tables_lower:
        return candidate_lower
    for expected_name in sorted(expected_tables_lower, key=len, reverse=True):
        if candidate_lower.startswith(expected_name + '_'):
            return expected_name
    return candidate_lower


def _glob_s3_files(duckdb_conn, s3_base: str, file_suffixes: tuple[str, ...], max_depth: int = 6) -> Set[str]:
    files: Set[str] = set()
    for suffix in file_suffixes:
        for depth in range(0, max_depth + 1):
            wildcard = "*/" * depth
            pattern = f"{s3_base}/{wildcard}*{suffix}"
            matches = [item[0] for item in duckdb_conn.execute("SELECT file FROM glob(?);", (pattern,)).fetchall()]
            files.update(matches)
    return files

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
        s3_base = file_dir.rstrip('/')
        file_suffixes = _allowed_suffixes(file_format)

        files = sorted(_glob_s3_files(duckdb_conn, s3_base=s3_base, file_suffixes=file_suffixes))
        table_names = {
            _extract_table_name_from_path(f, file_format).lower()
            for f in files
        }
        if len(files) == 0:
            LOGGER.warning(
                f"No S3 files matched under prefix '{s3_base}' with suffixes {file_suffixes}. "
                "Check BUCKET_NAME/BUCKET_PATH and exact case (S3 is case-sensitive)."
            )
        LOGGER.info(f"Table names found from S3 files: {table_names}")
        return table_names

    file_suffixes = _allowed_suffixes(file_format)

    if not multiple_file_per_table:
        table_names = {
            _extract_table_name_from_path(f, file_format).lower()
            for f in os.listdir(file_dir)
            if any(f.endswith(suffix) for suffix in file_suffixes)
        }
    else:
        files = []
        for root, _, filenames in os.walk(file_dir):
            files.extend([os.path.join(root, f) for f in filenames if any(f.endswith(suffix) for suffix in file_suffixes)])
        table_names = {
            _extract_table_name_from_path(f, file_format).lower()
            for f in files
        }
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
    expected_tables_lower = {item.lower() for item in cdm_tables_expected}
    canonical_table_names_from_files = {
        _canonicalize_table_name(item, expected_tables_lower)
        for item in table_names_from_files
    }
    optional_tables_lower = {item.lower() for item in OPTIONAL_TABLES}
    missing_tables = expected_tables_lower - canonical_table_names_from_files - optional_tables_lower
    if len(missing_tables) > 0:
        result = CheckResult(
            check_type = check_type,
            status = 'FAIL',
            table_name = tuple(sorted(missing_tables)),
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
    expected_tables_lower = {item.lower() for item in cdm_tables_expected}
    canonical_table_names_from_files = {
        _canonicalize_table_name(item, expected_tables_lower)
        for item in filenames_from_tables
    }
    extra_files = canonical_table_names_from_files - expected_tables_lower
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
