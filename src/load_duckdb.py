from typing import List
from src.util import get_csv_header, get_table_count, get_parquet_header, table_exists
from src.config import CONFIG, LOGGER
from duckdb import DuckDBPyConnection
import duckdb
from src.data_model import DataModel

def _quote_ident(name: str) -> str:
    """Quote an identifier so it survives DuckDB SQL parsing."""
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Quote a string literal so it survives DuckDB SQL parsing."""
    return "'" + value.replace("'", "''") + "'"


def drop_relation_if_exists(con: DuckDBPyConnection, table_name: str):
    """
    Drop a CDM relation whatever its type, so copy and pointer runs can alternate.

    DROP TABLE IF EXISTS raises on a view and DROP VIEW IF EXISTS raises on a table,
    so a run that re-uses an existing duckdb file has to look the type up first.
    """
    relation_type = con.execute(
        "SELECT table_type FROM information_schema.tables WHERE lower(table_name) = lower(?)",
        (table_name,)
    ).fetchone()
    if relation_type is None:
        return
    if relation_type[0] == 'VIEW':
        con.execute(f'DROP VIEW IF EXISTS {_quote_ident(table_name)};')
    else:
        con.execute(f'DROP TABLE IF EXISTS {_quote_ident(table_name)};')


def build_parquet_source_sql(parquet_paths) -> str:
    """
    Build the read_parquet() expression a pointer view selects from.

    Parameters:
    - parquet_paths: str or list of str, file paths, directories or globs.

    Multiple sources are combined with union_by_name so that parts written with
    differing column orders line up by name. Plain UNION ALL matches columns by
    position, which silently shifts values between columns when an unload writes
    its parts in different orders.
    """
    if isinstance(parquet_paths, str):
        parquet_paths = [parquet_paths]
    parquet_paths = list(parquet_paths)
    if not parquet_paths:
        raise ValueError("No parquet paths provided for pointer view.")
    path_list_sql = '[' + ', '.join(_quote_literal(p) for p in parquet_paths) + ']'
    return f"read_parquet({path_list_sql}, union_by_name = true)"


def create_parquet_pointer_view(
        parquet_path,
        con: DuckDBPyConnection,
        table_name: str,
        accept_additional_col: bool = True
    ):
    """
    Point a DuckDB view at parquet files instead of copying their rows into a table.

    The view is shaped like the table create_duckdb_tables() already built for this
    CDM table: columns are lower-cased, cast to the data model's declared types, and
    columns the data model defines but the file omits are selected as typed NULLs.
    Downstream checks therefore see the same columns and types they would after a
    COPY, without a second copy of the data on disk.

    Parameters:
    - parquet_path: str or list of str, path to the parquet file, directory or glob.
    - con: DuckDBPyConnection, a duckdb connection.
    - table_name: str, the name of the CDM table to expose.
    - accept_additional_col: bool, if True, expose columns present in the parquet but
      absent from the data model as VARCHAR. If False, raise instead.

    Returns:
    - duckdb.Connection object connected to the database.
    """
    source_sql = build_parquet_source_sql(parquet_path)

    # create_duckdb_tables() has already created an empty table from the data model,
    # so its DESCRIBE is the authority on the column names and types to expose.
    target_types = dict()
    if table_exists(con, table_name):
        target_types = {
            row[0].lower(): row[1]
            for row in con.execute(f'DESCRIBE {_quote_ident(table_name)}').fetchall()
        }

    parquet_header = [item[0] for item in con.execute(f'DESCRIBE SELECT * FROM {source_sql}').fetchall()]
    seen_columns = set()
    select_items = []
    for column_name in parquet_header:
        column_name_lower = column_name.lower()
        if column_name_lower in seen_columns:
            raise ValueError(
                f"Parquet for table {table_name} has duplicated column {column_name_lower} "
                "after case folding. The file will not be loaded."
            )
        seen_columns.add(column_name_lower)
        source_ref = _quote_ident(column_name)
        alias = _quote_ident(column_name_lower)
        if column_name_lower in target_types:
            select_items.append(f'CAST({source_ref} AS {target_types[column_name_lower]}) AS {alias}')
        elif accept_additional_col:
            select_items.append(f'CAST({source_ref} AS VARCHAR) AS {alias}')
            LOGGER.warning(f"column {column_name_lower} exists in parquet, but not in duckdb ddl. Exposed as '{column_name_lower} VARCHAR' in the pointer view. ")
        else:
            raise ValueError(f"Parquet file has additional column {column_name_lower} not in duckdb table {table_name} and accept_additional_col is set to False.")

    missing_columns = [col for col in target_types if col not in seen_columns]
    for column_name in missing_columns:
        # Keep the view shaped like the DDL table so column_exists() and the checks
        # behave the same in pointer mode as they do after a COPY.
        select_items.append(f'CAST(NULL AS {target_types[column_name]}) AS {_quote_ident(column_name)}')
    if missing_columns:
        LOGGER.warning(f"column(s) {missing_columns} defined in duckdb ddl for {table_name}, but not in parquet. Exposed as NULL in the pointer view. ")

    LOGGER.info(f"Pointing {parquet_path} at {table_name}...")
    view_sql = f'CREATE OR REPLACE VIEW {_quote_ident(table_name)} AS SELECT {", ".join(select_items)} FROM {source_sql};'
    LOGGER.debug(f"Executing SQL: {view_sql}")
    try:
        # the empty table from create_duckdb_tables() has to go before the view takes its name
        drop_relation_if_exists(con, table_name)
        con.execute(view_sql)
    except Exception as e:
        LOGGER.error(f"Fail to create pointer view: table={table_name}, parquet={parquet_path}")
        raise
    LOGGER.info(f"Pointed {table_name} at {len(parquet_header)} column(s) of parquet, {get_table_count(con, table_name)} rows visible.")
    return con


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
            # a previous pointer-mode run may have left a view under this name
            drop_relation_if_exists(con, t)
        sql += ddl_dict[t] + ';\n' 
    con.execute(sql)
    LOGGER.info(f"empty table(s) created -- {tables}")
    return con


def load_csv_to_duckdb(csv_path: str, con: DuckDBPyConnection, table_name: str, accept_additional_col: bool = True):
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
    csv_header = [item.lower() for item in get_csv_header(csv_path)]
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
    LOGGER.info(f"Loading {csv_path} to {table_name}...")
    copy_sql = f"""COPY {table_name} ({', '.join(csv_header)}) FROM '{csv_path}' ({CONFIG['duckdb']['copy_options'] + ', AUTO_DETECT false'});"""
    LOGGER.debug(f"Executing SQL: {copy_sql}")
    try:
        con.execute(copy_sql)
    except Exception as e:
        LOGGER.error(f"Fail to load CSV to DuckDB: table={table_name}, csv={csv_path}")
        raise
    count_after_load = get_table_count(con, table_name)
    LOGGER.info(f"Loaded {count_after_load - count_before_load} rows into {table_name}.")
    return con

def load_parquet_to_duckdb(parquet_path: str, con: DuckDBPyConnection, table_name: str, accept_additional_col: bool = True):
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
    parquet_header = [item.lower() for item in get_parquet_header(parquet_path)]
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
    LOGGER.info(f"Loading {parquet_path} to {table_name}...")
    copy_sql = f"""COPY {table_name} ({', '.join(parquet_header)}) FROM '{parquet_path}' ({CONFIG['duckdb']['copy_options']});"""
    LOGGER.debug(f"Executing SQL: {copy_sql}")
    try:
        con.execute(copy_sql)
    except Exception as e:
        LOGGER.error(f"Fail to load Parquet to DuckDB: table={table_name}, parquet={parquet_path}")
        raise
    count_after_load = get_table_count(con, table_name)
    LOGGER.info(f"Loaded {count_after_load - count_before_load} rows into {table_name}.")
    return con