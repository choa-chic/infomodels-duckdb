import csv
from typing import List, Dict
from src.config import CONFIG, LOGGER
import os
from src.constants import DQ_THRESHOLDS
import fnmatch
import duckdb


def get_csv_header(file_path: str, **kwargs) -> List[str]:
    """
    Get header column list from a csv file.

    Args:
        file_path (str): path to csv file.

    Raises:
        ValueError: If files is empty.

    Returns:
        list[str]: A list of strings, each representing a column name. 
    """
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile, kwargs)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"CSV file: {file_path} is empty.")
        else:
            return([x.lower() for x in header])

def get_parquet_header(file_path: str) -> List[str]:
    """
    Get header column list from a parquet file.

    Args:
        file_path (str): path to parquet file.
    Returns:
        list[str]: A list of strings, each representing a column name. 
    """
    with duckdb.connect(database=':memory:') as con:
        con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')")
        header = [item[0].lower() for item in con.fetchall()]
        LOGGER.debug(f"Parquet file: {file_path} has columns: {header}")
        return header

def get_table_count(
        con, 
        table_name: str, 
        schema: str = None
    ) -> int:
    """
    Get the count of rows in a DuckDB table.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to count rows in.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        int: The number of rows in the specified table.
    """
    if schema:
        sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
    else:
        sql = f"SELECT COUNT(*) FROM {table_name}"
    count = con.execute(sql).fetchone()[0]
    assert isinstance(count, int), f"Count for table {table_name} is not an integer: {count}"
    return count

def table_exists(con, table_name: str, schema: str = None) -> bool:
    """
    Check if a table exists in the DuckDB database.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to check.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    if schema:
        # the f-prefix used to be inside the string literal, which made this branch
        # send the text f'SHOW TABLES FROM "..."' to duckdb and always raise
        sql = f'SHOW TABLES FROM "{schema}";'
    else:
        sql = f"""
            SHOW TABLES;
        """
    all_tables = [item[0] for item in con.execute(sql).fetchall()]
    return table_name in all_tables

def column_exists(con, table_name: str, column_name: str, schema: str = None) -> bool:
    """
    Check if a column exists in a DuckDB table.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to check.
        column_name (str): The name of the column to check.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        bool: True if the column exists in the specified table, False otherwise.
    """
    if not table_exists(con, table_name, schema):
        return False
    if schema:
        sql = f"""
           DESCRIBE "{schema}"."{table_name}";
        """
    else:
        sql = f"""
            DESCRIBE "{table_name}";
        """
    all_columns = [item[0] for item in con.execute(sql).fetchall()]
    return column_name in all_columns

def get_threshold(check_type: str, default_threshold: float = 0, **kwargs):
    """
    Retrieve the threshold value for a given check type based on optional matching criteria.

    This function looks up a threshold value from a predefined global dictionary `DQ_THRESHOLDS`.
    It supports pattern-based matching using fnmatch for flexible filtering of conditions.

    Args:
        check_type (str): The type of data quality check (must be a key in DQ_THRESHOLDS).
        default_threshold (float, optional): The fallback threshold value if no criteria match.
        **kwargs: Arbitrary keyword arguments representing matching criteria.

    Returns:
        float: The matched threshold value based on criteria or the default if no match is found.

    Raises:
        ValueError: If the provided check_type is not present in DQ_THRESHOLDS.
    """
    result_threshold = default_threshold
    if check_type not in DQ_THRESHOLDS.keys():
        raise ValueError(f"check_type: {check_type} is not defined in threshold check_type. Supported values are: {DQ_THRESHOLDS.keys()}")
    for criterian in DQ_THRESHOLDS[check_type]:
        mismatch_detected = False
        for (search_arg, search_value) in kwargs.items():
            if search_arg in criterian.keys():
                if not fnmatch.fnmatch(search_value, criterian[search_arg]):
                    mismatch_detected = True
                    break
        if not mismatch_detected:
            result_threshold = criterian['threshold']
    return result_threshold