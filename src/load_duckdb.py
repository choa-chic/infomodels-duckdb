from typing import List
from src.util import get_csv_header, get_table_count, get_parquet_header
from src.config import CONFIG, LOGGER
from duckdb import DuckDBPyConnection
import duckdb
from src.data_model import DataModel


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def create_pointer_view_from_files(
    con: DuckDBPyConnection,
    table_name: str,
    source_files: list[str] | tuple[str, ...] | str,
    file_format: str,
):
    if isinstance(source_files, str):
        source_files = [source_files]
    if len(source_files) == 0:
        raise ValueError(f"No source files provided for pointer view: {table_name}")

    union_selects = []
    for source_path in source_files:
        source_path_sql = _escape_sql_string(source_path)
        if file_format == 'parquet':
            union_selects.append(f"SELECT * FROM read_parquet('{source_path_sql}')")
        elif file_format == 'csv':
            union_selects.append(f"SELECT * FROM read_csv('{source_path_sql}', header=true, auto_detect=true)")
        else:
            raise ValueError(f"Unsupported file_format for pointer mode: {file_format}")

    union_sql = "\nUNION ALL\n".join(union_selects)
    con.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    con.execute(f'DROP VIEW IF EXISTS "{table_name}";')
    con.execute(f'CREATE VIEW "{table_name}" AS {union_sql};')
    LOGGER.info(f"Created pointer view for {table_name} from {len(source_files)} source file(s).")

def init_duckdb_logging_schema(con: DuckDBPyConnection, run_id: str, run_config: dict, logging_schema = 'logging') -> DuckDBPyConnection:
    con.execute(f"""
    CREATE SCHEMA IF NOT EXISTS {logging_schema};
    CREATE TABLE IF NOT EXISTS {logging_schema}.dq (
        run_id VARCHAR,
        log_time TIMESTAMP,
        check_type VARCHAR,
        status VARCHAR,
        file_name VARCHAR,
        table_name VARCHAR,
        column_name VARCHAR,
        violation_pct FLOAT,
        threshold VARCHAR,
        message VARCHAR,
        extra_info VARCHAR
    );
    CREATE TABLE IF NOT EXISTS {logging_schema}.process (
        run_id VARCHAR,
        process_name VARCHAR,
        status VARCHAR,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS {logging_schema}.run (
        run_id VARCHAR,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        config STRING
    );
    """)
    # insert a new run
    con.execute(f"""INSERT INTO {logging_schema}.run (run_id, start_time, config) VALUES ('{run_id}', current_localtimestamp(), ?);""", (str(run_config),))
    return con

def create_duckdb_tables(data_model: DataModel, con: DuckDBPyConnection, skip_tables: List = [], recreate: bool = False):
    ddl_dict = data_model.to_duckdb_ddl()
    tables = set(ddl_dict.keys()) - set(skip_tables)
    sql = ''
    for t in tables:
        if recreate:
            sql += f'DROP TABLE IF EXISTS {t};\n'
        sql += ddl_dict[t] + ';\n' 
    con.execute(sql)
    LOGGER.info(f"empty table(s) created -- {tables}")
    return con


def load_csv_to_duckdb(csv_path: str | list[str] | tuple[str, ...], con: DuckDBPyConnection, table_name: str, accept_additional_col: bool = True):
    """
    Loads a CSV file into a DuckDB table. Any additional column in csv will be added to database

    Parameters:
    - csv_path: str, path to the CSV file.
    - con: DuckDBPyConnection, a duckdb connection
    - table_name: str, the name of the table to create/load into.
    - accept_additional_col: bool, if True, add additional columns in csv to duckdb. If False, will throw an error if addtional col in csv

    Returns:
    - duckdb.Connection object connected to the database.
    """
    csv_sources = list(csv_path) if isinstance(csv_path, (list, tuple)) else [csv_path]
    csv_header = [item.lower() for item in get_csv_header(csv_sources, duckdb_conn=con)]
    duckdb_columns = con.execute(f'DESCRIBE {table_name}').df()['column_name'].tolist()
    # if csv has more columns than duckdb
    if (set(csv_header) - set(duckdb_columns)):
        if accept_additional_col:
            for col in set(csv_header) - set(duckdb_columns):
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR;')
                LOGGER.warning(f"column {col} exists in csv, but not in duckdb ddl. Added '{col} VARCHAR' to duckdb. ")
        else:
            raise ValueError(f"CSV file has additional columns {set(csv_header) - set(duckdb_columns)} not in duckdb table {table_name} and accept_additional_col is set to False.")
    count_before_load = get_table_count(con, table_name)
    LOGGER.info(f"Loading {len(csv_sources)} csv file(s) to {table_name}...")
    for source_path in csv_sources:
        copy_sql = f"""COPY {table_name} ({', '.join(csv_header)}) FROM '{source_path}' ({CONFIG['duckdb']['copy_options'] + ', AUTO_DETECT false'});"""
        LOGGER.debug(f"Executing SQL: {copy_sql}")
        try:
            con.execute(copy_sql)
        except Exception:
            LOGGER.error(f"Fail to load CSV to DuckDB: table={table_name}, csv={source_path}")
            raise
    count_after_load = get_table_count(con, table_name)
    LOGGER.info(f"Loaded {count_after_load - count_before_load} rows into {table_name}.")
    return con

def load_parquet_to_duckdb(parquet_path: str | list[str] | tuple[str, ...], con: DuckDBPyConnection, table_name: str, accept_additional_col: bool = True):
    """
    Loads a Parquet file into a DuckDB table. Any additional column in parquet will be added to database

    Parameters:
    - parquet_path: str, path to the Parquet file or directory containing Parquet files.
    - con: DuckDBPyConnection, a duckdb connection
    - table_name: str, the name of the table to create/load into.
    - accept_additional_col: bool, if True, add additional columns in parquet to duckdb. If False, will throw an error if addtional col in parquet

    Returns:
    - duckdb.Connection object connected to the database.
    """
    parquet_sources = list(parquet_path) if isinstance(parquet_path, (list, tuple)) else [parquet_path]
    parquet_header = [item.lower() for item in get_parquet_header(parquet_sources, duckdb_conn=con)]
    duckdb_columns = con.execute(f'DESCRIBE {table_name}').df()['column_name'].tolist()
    # if parquet has more columns than duckdb
    if (set(parquet_header) - set(duckdb_columns)):
        if accept_additional_col:
            for col in set(parquet_header) - set(duckdb_columns):
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR;')
                LOGGER.warning(f"column {col} exists in parquet, but not in duckdb ddl. Added '{col} VARCHAR' to duckdb. ")
        else:
            raise ValueError(f"Parquet file has additional columns {set(parquet_header) - set(duckdb_columns)} not in duckdb table {table_name} and accept_additional_col is set to False.")
    count_before_load = get_table_count(con, table_name)
    LOGGER.info(f"Loading {len(parquet_sources)} parquet file(s) to {table_name}...")
    for source_path in parquet_sources:
        copy_sql = f"""COPY {table_name} ({', '.join(parquet_header)}) FROM '{source_path}' ({CONFIG['duckdb']['copy_options']});"""
        LOGGER.debug(f"Executing SQL: {copy_sql}")
        try:
            con.execute(copy_sql)
        except Exception:
            LOGGER.error(f"Fail to load Parquet to DuckDB: table={table_name}, parquet={source_path}")
            raise
    count_after_load = get_table_count(con, table_name)
    LOGGER.info(f"Loaded {count_after_load - count_before_load} rows into {table_name}.")
    return con