"""
Second stage of a pointer-mode run: turn the views into tables, once the checks are in.

`python -m src.main` with access_mode 'pointer' leaves the CDM as views over the submission
files, so the checks cost no disk. This stage takes the duckdb file that run produced and
subsumes the submission data into it, so the file can be submitted on its own.

Splitting it in two is what makes consuming safe to offer: the DQ results are already in the
file by the time anyone decides to delete the submission files, so the decision is made by a
human reading the report rather than by the tool guessing.

    python -m src.materialize                 # views -> tables, submission files untouched
    python -m src.materialize --consume       # ... and delete each file once its rows are in
    python -m src.materialize --consume --force   # ... even though the run recorded failures

The duckdb file and data model come from the same config.yml the check run used.
"""
import argparse
import sys

import duckdb

from src.config import CONFIG, LOGGER
from src.data_model import DataModel
from src.load_duckdb import (
    get_dq_failure_count,
    get_latest_run_id,
    get_pointer_sources,
    list_pointer_views,
    materialize_pointer_views,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog='python -m src.materialize',
        description="Materialize the pointer views in a duckdb file into real tables.",
    )
    parser.add_argument(
        '--consume', action='store_true',
        help="Delete each submission file once its rows are in the duckdb file. Destructive and unrecoverable.",
    )
    parser.add_argument(
        '--force', action='store_true',
        help="Consume even though the run recorded DQ failures. Ignored without --consume.",
    )
    parser.add_argument(
        '--run-id', default=None,
        help="Read the DQ results and submission paths of this run instead of the latest one.",
    )
    return parser.parse_args(argv)


def materialize(consume: bool = False, force: bool = False, run_id: str = None) -> dict:
    """
    Materialize the pointer views held in the configured duckdb file.

    Returns the summary dict from materialize_pointer_views().
    """
    data_model = DataModel(**CONFIG['data-models'])
    duckdb_path = CONFIG['duckdb']['path']
    LOGGER.info(f"Materializing pointer views in {duckdb_path} (consume={consume}).")
    with duckdb.connect(duckdb_path) as con:
        if CONFIG['duckdb'].get('memory_limit', None):
            con.execute(f"SET memory_limit='{CONFIG['duckdb']['memory_limit']}'")
        con.execute("SET preserve_insertion_order=false")

        if run_id is None:
            run_id = get_latest_run_id(con)
        views = list_pointer_views(con, data_model)
        if not views:
            LOGGER.warning(
                f"No pointer views found in {duckdb_path}. Nothing to materialize -- "
                "was the check run made with access_mode 'pointer'?"
            )
            return {'tables': [], 'rows': 0, 'removed': [], 'unknown_source': []}
        LOGGER.info(f"Found {len(views)} pointer view(s) from run {run_id}.")

        if consume:
            failure_count = get_dq_failure_count(con, run_id=run_id)
            if failure_count > 0 and not force:
                # the submission files are what is needed to diagnose the failures, and
                # deleting them cannot be undone -- so this stops rather than warns
                raise SystemExit(
                    f"Run {run_id} recorded {failure_count} DQ failure(s). Refusing to delete the "
                    "submission files.\nReview them with: SELECT * FROM logging.dq WHERE status = 'FAIL';\n"
                    "Re-run without --consume to materialize only, or with --force to consume anyway."
                )
            if failure_count > 0:
                LOGGER.warning(f"Consuming submission files despite {failure_count} DQ failure(s) in run {run_id} (--force).")

        summary = materialize_pointer_views(
            con=con,
            data_model=data_model,
            consume=consume,
            sources=get_pointer_sources(con, run_id=run_id) if consume else None,
        )
    LOGGER.info(f"{duckdb_path} now holds {summary['rows']} row(s) across {len(summary['tables'])} table(s).")
    if summary['unknown_source']:
        LOGGER.warning(f"No recorded source for {summary['unknown_source']}; those files were left in place.")
    return summary


def main(argv=None):
    args = parse_args(argv)
    if args.force and not args.consume:
        LOGGER.warning("--force has no effect without --consume.")
    materialize(consume=args.consume, force=args.force, run_id=args.run_id)


if __name__ == '__main__':
    main(sys.argv[1:])
