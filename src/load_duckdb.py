from typing import List
import os
import shutil
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


def get_relation_type(con: DuckDBPyConnection, table_name: str):
    """
    Return 'VIEW', 'BASE TABLE' or None for a relation, so callers can branch on its type.
    """
    row = con.execute(
        "SELECT table_type FROM information_schema.tables WHERE lower(table_name) = lower(?)",
        (table_name,)
    ).fetchone()
    return None if row is None else row[0]


def drop_relation_if_exists(con: DuckDBPyConnection, table_name: str):
    """
    Drop a CDM relation whatever its type, so copy and pointer runs can alternate.

    DROP TABLE IF EXISTS raises on a view and DROP VIEW IF EXISTS raises on a table,
    so a run that re-uses an existing duckdb file has to look the type up first.
    """
    relation_type = get_relation_type(con, table_name)
    if relation_type is None:
        return
    if relation_type == 'VIEW':
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


def record_pointer_source(con: DuckDBPyConnection, run_id: str, table_name: str, source_path: str, logging_schema: str = 'logging'):
    """
    Record which submission path a pointer view reads.

    A view knows its parquet only inside its own SQL. Writing the mapping into the duckdb
    file lets a later process -- a separate materialize stage, run once a human has read the
    DQ results -- know what it is allowed to delete without parsing view definitions.
    """
    con.execute(
        f"INSERT INTO {logging_schema}.pointer_source (run_id, log_time, table_name, source_path) "
        "VALUES (?, current_localtimestamp(), ?, ?);",
        (run_id, table_name, source_path)
    )


def get_pointer_sources(con: DuckDBPyConnection, run_id: str = None, logging_schema: str = 'logging') -> dict:
    """
    Return {table_name: source_path} for pointer views, most recent record per table.

    Defaults to the latest run, which is the one whose views are currently in the file.
    """
    if not table_exists(con, 'pointer_source', schema=logging_schema):
        return dict()
    if run_id is None:
        run_id = get_latest_run_id(con, logging_schema=logging_schema)
    if run_id is None:
        return dict()
    rows = con.execute(
        f"SELECT table_name, source_path FROM {logging_schema}.pointer_source "
        "WHERE run_id = ? QUALIFY row_number() OVER (PARTITION BY table_name ORDER BY log_time DESC) = 1;",
        (run_id,)
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def get_latest_run_id(con: DuckDBPyConnection, logging_schema: str = 'logging') -> str:
    """Return the run_id of the most recently started run, or None if the file has none."""
    if not table_exists(con, 'run', schema=logging_schema):
        return None
    row = con.execute(f"SELECT run_id FROM {logging_schema}.run ORDER BY start_time DESC LIMIT 1;").fetchone()
    return None if row is None else row[0]


def get_dq_failure_count(con: DuckDBPyConnection, run_id: str = None, logging_schema: str = 'logging') -> int:
    """
    Count FAIL rows recorded by a run, so a later stage can see how the checks went.

    Defaults to the latest run.
    """
    if not table_exists(con, 'dq', schema=logging_schema):
        return 0
    if run_id is None:
        run_id = get_latest_run_id(con, logging_schema=logging_schema)
    if run_id is None:
        return 0
    row = con.execute(
        f"SELECT count(*) FROM {logging_schema}.dq WHERE run_id = ? AND status = 'FAIL';",
        (run_id,)
    ).fetchone()
    return 0 if row is None else row[0]


def list_pointer_views(con: DuckDBPyConnection, data_model: DataModel) -> List[str]:
    """Return the CDM tables currently exposed as views, in a stable order."""
    return sorted(
        table for table in data_model.all_table_names()
        if get_relation_type(con, table) == 'VIEW'
    )


def materialize_pointer_view(con: DuckDBPyConnection, table_name: str, data_model: DataModel) -> int:
    """
    Replace a pointer view with a real table holding the same rows.

    Pointer mode leaves the rows in the parquet, which is what makes the checks cheap,
    but a submission has to be a single duckdb file that carries its own data. The view
    is already cast to the data model's declared types, so selecting from it in the data
    model's column order reproduces the table copy mode would have built, column for
    column. Columns the parquet added are appended as copy mode appends them.

    The swap is staged: the new table is built and its row count verified before the view
    is dropped, so a failure part way through leaves the view intact.

    Parameters:
    - con: DuckDBPyConnection, a duckdb connection.
    - data_model: DataModel, the authority on column order for this table.
    - table_name: str, the name of the CDM relation to materialize.

    Returns:
    - int, the number of rows materialized.
    """
    relation_type = get_relation_type(con, table_name)
    if relation_type is None:
        raise ValueError(f"No relation named {table_name} to materialize.")
    if relation_type != 'VIEW':
        # already a table -- a copy-mode run, or a re-run over an already materialized file
        LOGGER.debug(f"{table_name} is already a {relation_type}, nothing to materialize.")
        return get_table_count(con, table_name)

    view_columns = [row[0] for row in con.execute(f'DESCRIBE {_quote_ident(table_name)}').fetchall()]
    model_columns = [col for col in data_model.all_column_names_in_table(table_name) if col in view_columns]
    extra_columns = [col for col in view_columns if col not in model_columns]
    ordered_columns = model_columns + extra_columns
    select_list = ', '.join(_quote_ident(col) for col in ordered_columns)

    expected_rows = get_table_count(con, table_name)
    staging_name = f'_materialize_{table_name}'
    LOGGER.info(f"Materializing view {table_name} into a table ({expected_rows} rows)...")
    drop_relation_if_exists(con, staging_name)
    try:
        con.execute(f'CREATE TABLE {_quote_ident(staging_name)} AS SELECT {select_list} FROM {_quote_ident(table_name)};')
        materialized_rows = get_table_count(con, staging_name)
        if materialized_rows != expected_rows:
            raise ValueError(
                f"Materializing {table_name} produced {materialized_rows} rows but the view had "
                f"{expected_rows}. The view has been left in place."
            )
        # only now is it safe to give up the view
        con.execute(f'DROP VIEW {_quote_ident(table_name)};')
        con.execute(f'ALTER TABLE {_quote_ident(staging_name)} RENAME TO {_quote_ident(table_name)};')
    except Exception:
        LOGGER.error(f"Fail to materialize pointer view: table={table_name}")
        drop_relation_if_exists(con, staging_name)
        raise
    LOGGER.info(f"Materialized {table_name}: {materialized_rows} rows now held in the duckdb file.")
    return materialized_rows


def remove_submission_source(path: str):
    """
    Delete a submission file or part directory once its rows are held in the duckdb file.

    This is what makes pointer mode cheaper on peak disk than copy mode when the deliverable
    has to carry all the data: without it the parquet and the full duckdb coexist, exactly as
    they do in copy mode. It is destructive and unrecoverable, so callers must materialize and
    verify first.
    """
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)
    LOGGER.info(f"Removed submission source {path}; its rows are now in the duckdb file.")


def materialize_pointer_views(
        con: DuckDBPyConnection,
        data_model: DataModel,
        consume: bool = False,
        sources: dict = None
    ) -> dict:
    """
    Turn every pointer view in the file into a table, optionally deleting its source.

    Shared by the one-shot path in main() and by the separate materialize stage, so both
    produce the same duckdb file. Each table is materialized and verified before its source
    is removed, and a table whose source is unknown is materialized but never deleted.

    Parameters:
    - con: DuckDBPyConnection, a duckdb connection.
    - data_model: DataModel, the authority on column order.
    - consume: bool, if True delete each submission source once its rows are in the file.
    - sources: dict of {table_name: source_path}; required for consume.

    Returns:
    - dict summary with the tables materialized, rows written and sources removed.
    """
    sources = sources or dict()
    views = list_pointer_views(con, data_model)
    if not views:
        LOGGER.info("No pointer views to materialize.")
        return {'tables': [], 'rows': 0, 'removed': [], 'unknown_source': []}
    materialized, removed, unknown_source, total_rows = [], [], [], 0
    for table_name in views:
        total_rows += materialize_pointer_view(con=con, table_name=table_name, data_model=data_model)
        materialized.append(table_name)
        if not consume:
            continue
        source_path = sources.get(table_name)
        if source_path is None:
            # never guess at what to delete
            LOGGER.warning(f"No recorded submission source for {table_name}; materialized but not consumed.")
            unknown_source.append(table_name)
            continue
        remove_submission_source(source_path)
        removed.append(source_path)
    # flush the freshly written rows out of the WAL and into the duckdb file itself
    con.execute("CHECKPOINT;")
    LOGGER.info(f"Materialized {len(materialized)} table(s), {total_rows} rows total. Removed {len(removed)} submission source(s).")
    return {'tables': materialized, 'rows': total_rows, 'removed': removed, 'unknown_source': unknown_source}


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
    CREATE TABLE IF NOT EXISTS {logging_schema}.pointer_source (
        run_id VARCHAR,
        log_time TIMESTAMP,
        table_name VARCHAR,
        source_path VARCHAR
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