from src.config import CONFIG, LOGGER
from src.dq_checks.check_result import CheckResult
from src.load_duckdb import create_duckdb_tables, load_csv_to_duckdb, init_duckdb_logging_schema, load_parquet_to_duckdb, create_parquet_pointer_view, materialize_pointer_views, record_pointer_source, get_pointer_sources
from src.data_model import DataModel
from src.constants import OPTIONAL_TABLES
from src.dq_checks.check_file_completeness import check_missing_submission_file, check_extra_submission_file
from src.dq_checks.check_header import check_duplicated_column_in_csv, check_extra_column_in_csv, check_missing_column_in_csv, check_extra_column_in_parquet, check_missing_column_in_parquet
from src.dq_checks.check_fk import check_fk_violation
from src.dq_checks.check_not_null import check_not_null_violation
from src.dq_checks.check_distinct import check_distinct_violation
from src.dq_checks.check_fact_relationship import check_fact_relationship
import duckdb
import os
import fnmatch
from datetime import datetime

class _Context():
    '''An explicit context class to hold dynamic state variables during the DQ run.'''
    def __init__(
        self, 
        run_id: str, 
        skip_check_tables: list = [],
        skip_check_columns: dict = {}, # a dict of {table_name: (column_name, ...)}
        skip_duckdb_load_tables: list = [],
        **kwargs
    ):
        self.run_id = run_id
        self.skip_check_tables = skip_check_tables
        self.skip_duckdb_load_tables = skip_duckdb_load_tables
        self.skip_check_columns = skip_check_columns
        for key, value in kwargs.items():
            setattr(self, key, value)

def main():
    run_id = CONFIG['core'].get(
        'run_id', 
        datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
    )
    CheckResult.run_id = run_id
    context = _Context(run_id=run_id) # initialize context.
    with duckdb.connect(CONFIG['duckdb']['path']) as con:
        if CONFIG['duckdb'].get('memory_limit', None):
            con.execute(f"SET memory_limit='{CONFIG['duckdb']['memory_limit']}'")
        con.execute("SET preserve_insertion_order=false")
        init_duckdb_logging_schema(con, run_id, CONFIG)
        LOGGER.info(f"Run ID: {run_id}.\nRunning with config: " + str(CONFIG))  
        # get data models
        LOGGER.info(f"Loading data models with config: {CONFIG['data-models']}")
        data_model = DataModel(**CONFIG['data-models'])
        #data_models_dict = data_model.data
        LOGGER.info("Data models loaded successfully. ")

        context.skip_check_tables = list(OPTIONAL_TABLES)
        context.skip_check_columns = dict() # a dict of {table_name: (column_name, ...)}
        _skip_duckdb_load_table_patterns = list(CONFIG['duckdb'].get('skip_load', []))
        context.skip_duckdb_load_tables = [table for table in data_model.all_table_names() if any(fnmatch.fnmatch(table, pattern) for pattern in _skip_duckdb_load_table_patterns)]
        LOGGER.debug(f"Tables to skip loading into DuckDB from config: {context.skip_duckdb_load_tables}")

        # Initialize DuckDB database
        LOGGER.info("Initializing DuckDB database.")
        create_duckdb_tables(data_model, con, skip_tables = context.skip_duckdb_load_tables, recreate = True)
        LOGGER.info("DuckDB tables created successfully.")

        # check submission files completeness
        submission_dir = CONFIG['submission_files']['dir']
        submission_file_format = CONFIG['submission_files'].get('file_format', 'csv')
        if_multiple_file_per_table = CONFIG['submission_files'].get('multiple_file_per_table', False)
        # 'copy' materializes rows into duckdb tables; 'pointer' leaves the rows in the
        # submission files and exposes them as views, so no second copy lands on disk.
        access_mode = CONFIG['submission_files'].get('access_mode', 'copy')
        if access_mode not in ('copy', 'pointer'):
            raise ValueError(f"Unsupported submission_files.access_mode: {access_mode}. Supported values are 'copy' and 'pointer'.")
        if access_mode == 'pointer' and submission_file_format != 'parquet':
            raise NotImplementedError(f"access_mode 'pointer' is only implemented for parquet submissions, not {submission_file_format}.")
        # What to do with the pointer views once the checks have run. 'off' leaves them as
        # views; 'keep' and 'consume' copy their rows into the duckdb file so the file can be
        # submitted on its own, and 'consume' additionally deletes each submission file once
        # its rows are safely in the file, which is what keeps peak disk below copy mode's.
        materialize_mode = CONFIG['submission_files'].get('materialize', 'off')
        if materialize_mode not in ('off', 'keep', 'consume'):
            raise ValueError(f"Unsupported submission_files.materialize: {materialize_mode}. Supported values are 'off', 'keep' and 'consume'.")
        if materialize_mode != 'off' and access_mode != 'pointer':
            raise ValueError(f"submission_files.materialize is only meaningful with access_mode 'pointer', not '{access_mode}'.")
        # Deleting the submission after a failed run destroys what is needed to diagnose it,
        # so a one-shot consume stops at the first DQ failure unless this is set. The two
        # stage flow (python -m src.materialize --consume --force) is the better way to
        # override, since by then the DQ results are in the file to be read first.
        consume_with_dq_failures = CONFIG['submission_files'].get('consume_with_dq_failures', False)

        LOGGER.debug("Checking submission files completeness.")
        required_cdm_tables = tuple(set(data_model.all_table_names()) - set(OPTIONAL_TABLES) - set(context.skip_duckdb_load_tables))
        check_result_missing_submission_file = check_missing_submission_file(
            file_dir = submission_dir,
            cdm_tables_expected = required_cdm_tables,
            file_format = submission_file_format,
            multiple_file_per_table = if_multiple_file_per_table,
            duckdb_conn = con
        )
        if check_result_missing_submission_file.status not in ('PASS', 'SKIPPED'):
            # skip checks for missing tables
            context.skip_check_tables.extend(check_result_missing_submission_file.table_name)
            context.skip_duckdb_load_tables.extend(check_result_missing_submission_file.table_name)

        LOGGER.debug("Checking for extra submission files.")
        check_result_extra_submission_file = check_extra_submission_file(
            file_dir = submission_dir,
            cdm_tables_expected = data_model.all_table_names(),
            file_format = submission_file_format,
            multiple_file_per_table = if_multiple_file_per_table,
            duckdb_conn = con
        )

        # Check header issues
        if submission_file_format not in ('csv', 'parquet'):
            raise ValueError(f"Unsupported submission file format: {submission_file_format}. Supported formats are 'csv' and 'parquet'.")
        # TODO: implement csv header checks for multiple files per table later
        if submission_file_format == 'csv' and if_multiple_file_per_table:
            raise NotImplementedError("Header checks for multiple files per table in CSV format are not implemented yet. For now, please merge your csv files into single file.")
        if submission_file_format == 'csv' and not if_multiple_file_per_table:
            submission_file_extension = '.csv'
            # check header issues
            for table_name in data_model.all_table_names():
                # check if file exists for the table
                file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
                if not os.path.isfile(file_path):
                    LOGGER.debug(f"No submission file found for table {table_name}. Skipping header checks. Path: {file_path}")
                    continue
                LOGGER.debug(f"Checking header for table: {table_name}, file: {file_path}")
                # check duplicated columns in csv
                check_result_duplicated_column = check_duplicated_column_in_csv(file_path, table_name)
                if check_result_duplicated_column.status != 'PASS':
                    # if the csv has duplicated columns, don't load the table to duckdb
                    context.skip_duckdb_load_tables.append(table_name)
                    context.skip_check_tables.append(table_name)
                # check extra columns in csv
                check_result_extra_column = check_extra_column_in_csv(file_path, data_model, table_name, duckdb_conn=con)
                # check missing columns in csv
                check_result_missing_column = check_missing_column_in_csv(file_path, data_model, table_name, duckdb_conn=con)
                if check_result_missing_column.status != 'PASS':
                    context.skip_check_columns[table_name] = context.skip_check_columns.get(table_name, tuple()) + check_result_missing_column.column_name
        if submission_file_format == 'parquet':
            if if_multiple_file_per_table:
                submission_file_extension = ''
            else:
                submission_file_extension = '.parquet'
            # check header issues
            for table_name in data_model.all_table_names():
                file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
                # check if file_path exists
                if not os.path.exists(file_path):
                    LOGGER.debug(f"No submission file found for table {table_name}. Skipping header checks. Path: {file_path}")
                    continue
                LOGGER.debug(f"Checking header for table: {table_name}, file: {file_path}")
                # check extra columns in parquet
                check_result_extra_column = check_extra_column_in_parquet(file_path, data_model, table_name, duckdb_conn=con)
                # check missing columns in parquet
                check_result_missing_column = check_missing_column_in_parquet(file_path, data_model, table_name, duckdb_conn=con)
                if check_result_missing_column.status != 'PASS':
                    context.skip_check_columns[table_name] = context.skip_check_columns.get(table_name, tuple()) + check_result_missing_column.column_name
        # Load submission files into DuckDB
        LOGGER.info(f"Preparing submission files in DuckDB with access_mode={access_mode}.")
        # TODO: implement csv load for multiple files per table later
        if submission_file_format == 'csv' and if_multiple_file_per_table:
            raise NotImplementedError("Loading multiple files per table in CSV format into DuckDB is not implemented yet. Please merge your csv files into single file per table.")
        if submission_file_format == 'csv' and not if_multiple_file_per_table:
            submission_file_extension = '.csv'
            # Loading csv files to DuckDB
            for table_name in data_model.all_table_names():
                file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
                if not os.path.isfile(file_path):
                    LOGGER.debug(f"No submission file found for table {table_name}. Skipping DuckDB load. Path: {file_path}")
                    continue
                if table_name in context.skip_duckdb_load_tables:
                    LOGGER.debug(f"Skipping loading {table_name} to DuckDB as it is in the skip list.")
                    continue
                LOGGER.info(f"Loading {file_path} into DuckDB table {table_name}.")
                load_csv_to_duckdb(csv_path=file_path, con=con, table_name=table_name, accept_additional_col=True)
        if submission_file_format == 'parquet':
            if if_multiple_file_per_table:
                submission_file_extension = ''
            else:
                submission_file_extension = '.parquet'
            # Loading parquet files to DuckDB
            for table_name in data_model.all_table_names():
                file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
                # check if file_path exists
                if not os.path.exists(file_path):
                    LOGGER.debug(f"No submission file found for table {table_name}. Skipping DuckDB load. Path: {file_path}")
                    continue
                if table_name in context.skip_duckdb_load_tables:
                    LOGGER.debug(f"Skipping loading {table_name} to DuckDB as it is in the skip list.")
                    continue
                if access_mode == 'pointer':
                    create_parquet_pointer_view(parquet_path=file_path, con=con, table_name=table_name, accept_additional_col=True)
                    # persisted, so a later `python -m src.materialize` knows what it may delete
                    record_pointer_source(con, run_id, table_name, file_path)
                else:
                    LOGGER.info(f"Loading {file_path} into DuckDB table {table_name}.")
                    load_parquet_to_duckdb(parquet_path=file_path, con=con, table_name=table_name, accept_additional_col=True)


        LOGGER.info(f"All submission files prepared in DuckDB successfully with access_mode={access_mode}.")
        
        # Check foreign key violations
        LOGGER.info("Checking foreign key violations.") 
        for fk_definition in data_model.data['schema']['constraints']['foreign_keys']:
            main_table = fk_definition['source_table']
            main_column = fk_definition['source_field']
            reference_table = fk_definition['target_table']
            reference_column = fk_definition['target_field']
            if main_table in context.skip_check_tables:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as main table is in the skip list.")
                continue
            if reference_table in context.skip_check_tables or reference_table in context.skip_check_tables:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as reference table is in the skip list.")
                continue
            if main_table in context.skip_check_columns.keys() and main_column in context.skip_check_columns[main_table]:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as the main column is in the skip list.")
                continue
            if reference_table in context.skip_check_columns.keys() and reference_column in context.skip_check_columns[reference_table]:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as the reference column is in the skip list.")
                continue
            check_result_fk = check_fk_violation(
                con=con,
                main_table=main_table,
                main_column=main_column,
                reference_table=reference_table,
                reference_column=reference_column,
            )
            LOGGER.debug(f"Foreign Key Check Finished.")
        
        # Check Not Null violations
        for not_null_definition in data_model.data['schema']['constraints']['not_null']:
            table_name = not_null_definition['table']
            column_name = not_null_definition['field']

            if table_name in context.skip_check_tables:
                LOGGER.debug(f"Skipping Not Null check for {table_name}.{column_name} as table is in the skip list.")
                continue
            if table_name in context.skip_check_columns.keys() and column_name in context.skip_check_columns[table_name]:
                LOGGER.debug(f"Skipping Not Null check for {table_name}.{column_name} as column is in the skip list.")
                continue
            check_result_not_null = check_not_null_violation(
                con=con,
                table_name=table_name,
                column_name=column_name,
            )
            LOGGER.debug(f"Not Null Check Finished.")

        # Check Distinct violations
        for distinct_definition in data_model.data['schema']['constraints']['uniques']:
            table_name = distinct_definition['table']
            column_name = distinct_definition['field']

            if table_name in context.skip_check_tables:
                LOGGER.debug(f"Skipping Distinct check for {table_name}.{column_name} as table is in the skip list.")
                continue
            if table_name in context.skip_check_columns.keys() and column_name in context.skip_check_columns[table_name]:
                LOGGER.debug(f"Skipping Distinct check for {table_name}.{column_name} as column is in the skip list.")
                continue
            check_result_distinct = check_distinct_violation(
                con=con,
                table_name=table_name,
                column_names=column_name,
            )
            LOGGER.debug(f"Distinct Check Finished.")
        
        # Check PK violations
        # PK is a combination of NOT NULL and DISTINCT
        for pk_definition in data_model.data['schema']['constraints']['primary_keys']:
            table_name = pk_definition['table']
            column_names = tuple(pk_definition['fields'])  # list of columns in the primary key

            if table_name in context.skip_check_tables:
                LOGGER.debug(f"Skipping Primary Key check for {table_name}({', '.join(column_names)}) as table is in the skip list.")
                continue
            if table_name in context.skip_check_columns.keys() and any(col in context.skip_check_columns[table_name] for col in column_names):
                LOGGER.debug(f"Skipping Primary Key check for {table_name}({', '.join(column_names)}) as one or more columns are in the skip list.")
                continue
            # check not null for each column in the primary key
            for column_name in column_names:
                check_result_pk_not_null = check_not_null_violation(
                    con=con,
                    table_name=table_name,
                    column_name=column_name,
                )
                LOGGER.debug(f"Primary Key Not Null Check Finished for {table_name}.{column_name}.")
            # check distinct for the combination of columns in the primary key
            check_result_pk_distinct = check_distinct_violation(
                con=con,
                table_name=table_name,
                column_names=column_names
            )
            LOGGER.debug(f"Primary Key Distinct Check Finished for {table_name}({', '.join(column_names)}).")
            LOGGER.debug(f"Primary Key Check Finished for {table_name}.{column_names}.")

        # Check fact_relationship
        check_result_fact_relationship = check_fact_relationship(
            con=con,
            skip_tables = context.skip_check_tables
        )
        
        # Summarize DQ results
        CheckResult.summary(LOGGER)

        # Subsume the submission files into the duckdb file, now that every check has run.
        # This has to come after the checks: the views are live handles on the parquet, so a
        # table consumed early would pull the file out from under a check that still needs it.
        # Leaving materialize 'off' and running `python -m src.materialize` afterwards does the
        # same thing as a separate stage, with the DQ results available to read in between.
        if materialize_mode != 'off':
            consume = materialize_mode == 'consume'
            if consume and len(CheckResult.dq_fail) > 0 and not consume_with_dq_failures:
                LOGGER.warning(
                    f"{len(CheckResult.dq_fail)} DQ failure(s) in this run; materializing the submission "
                    "files but keeping them on disk. Review logging.dq, then consume them with "
                    "`python -m src.materialize --consume --force` if they are expected."
                )
                consume = False
            elif consume and len(CheckResult.dq_fail) > 0:
                LOGGER.warning(f"Consuming submission files despite {len(CheckResult.dq_fail)} DQ failure(s) (consume_with_dq_failures).")
            LOGGER.info(f"Materializing pointer views into the duckdb file (materialize={materialize_mode}).")
            materialize_pointer_views(
                con=con,
                data_model=data_model,
                consume=consume,
                sources=get_pointer_sources(con, run_id=run_id) if consume else None,
            )

        # Exit with code 1 if there is any DQ failure        
        if len(CheckResult.dq_fail) > 0:
            exit(1)



if __name__ == '__main__':
    main()
