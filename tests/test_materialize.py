import os

import duckdb
import pytest

from src.config import CONFIG
from src.data_model import DataModel
from src.load_duckdb import (
    create_duckdb_tables,
    create_parquet_pointer_view,
    get_dq_failure_count,
    get_latest_run_id,
    get_pointer_sources,
    get_relation_type,
    init_duckdb_logging_schema,
    list_pointer_views,
    materialize_pointer_views,
    record_pointer_source,
)
from src.materialize import materialize, parse_args

DATA_MODEL_PATH = 'tests/data/data_model/pedsnet_v57_data_model.json'
TABLE = 'care_site'
RUN_ID = 'test_run'


@pytest.fixture
def data_model():
    return DataModel(mode='json', name='pedsnet', version='5.7.0', file_path=DATA_MODEL_PATH)


def _write_parquet(tmp_path, name, select_sql):
    path = tmp_path / name
    with duckdb.connect() as writer:
        writer.execute(f"COPY ({select_sql}) TO '{path}' (FORMAT PARQUET)")
    return str(path)


def _staged_run(tmp_path, data_model, db_name='cdm.duckdb', failures=0):
    """Build the duckdb file a pointer-mode check run would leave behind."""
    db_path = str(tmp_path / db_name)
    parquet = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id, 'clinic' AS care_site_name")
    con = duckdb.connect(db_path)
    init_duckdb_logging_schema(con, RUN_ID, {})
    create_duckdb_tables(data_model, con, recreate=True)
    create_parquet_pointer_view(parquet_path=parquet, con=con, table_name=TABLE)
    record_pointer_source(con, RUN_ID, TABLE, parquet)
    for i in range(failures):
        con.execute(
            "INSERT INTO logging.dq (run_id, log_time, check_type, status) "
            "VALUES (?, current_localtimestamp(), ?, 'FAIL');", (RUN_ID, f'check_{i}')
        )
    con.close()
    return db_path, parquet


def test_views_and_sources_survive_process_exit(tmp_path, data_model):
    """The whole two-stage design rests on this: stage 2 opens a file stage 1 closed."""
    db_path, parquet = _staged_run(tmp_path, data_model)

    con = duckdb.connect(db_path)
    assert list_pointer_views(con, data_model) == [TABLE]
    assert get_pointer_sources(con) == {TABLE: parquet}
    assert get_latest_run_id(con) == RUN_ID
    con.close()


def test_get_dq_failure_count_reads_the_recorded_run(tmp_path, data_model):
    db_path, _ = _staged_run(tmp_path, data_model, failures=3)
    con = duckdb.connect(db_path)
    assert get_dq_failure_count(con) == 3
    con.close()


def test_get_dq_failure_count_is_zero_on_a_clean_run(tmp_path, data_model):
    db_path, _ = _staged_run(tmp_path, data_model)
    con = duckdb.connect(db_path)
    assert get_dq_failure_count(con) == 0
    con.close()


def test_materialize_pointer_views_consumes_recorded_sources(tmp_path, data_model):
    db_path, parquet = _staged_run(tmp_path, data_model)
    con = duckdb.connect(db_path)

    summary = materialize_pointer_views(con, data_model, consume=True, sources=get_pointer_sources(con))

    assert summary['tables'] == [TABLE]
    assert summary['rows'] == 1
    assert summary['removed'] == [parquet]
    assert not os.path.exists(parquet)
    assert get_relation_type(con, TABLE) == 'BASE TABLE'
    con.close()


def test_materialize_pointer_views_never_deletes_an_unknown_source(tmp_path, data_model):
    """A table whose source was not recorded is materialized, never guessed at."""
    db_path, parquet = _staged_run(tmp_path, data_model)
    con = duckdb.connect(db_path)

    summary = materialize_pointer_views(con, data_model, consume=True, sources={})

    assert summary['unknown_source'] == [TABLE]
    assert summary['removed'] == []
    assert os.path.exists(parquet)
    assert get_relation_type(con, TABLE) == 'BASE TABLE'
    con.close()


def test_materialize_pointer_views_is_a_noop_without_views(tmp_path, data_model):
    db_path, _ = _staged_run(tmp_path, data_model)
    con = duckdb.connect(db_path)
    materialize_pointer_views(con, data_model, consume=False)

    summary = materialize_pointer_views(con, data_model, consume=False)

    assert summary['tables'] == [] and summary['rows'] == 0
    con.close()


def _point_config_at(monkeypatch, db_path):
    monkeypatch.setitem(CONFIG['duckdb'], 'path', db_path)
    monkeypatch.setitem(CONFIG, 'data-models', {
        'mode': 'json', 'name': 'pedsnet', 'version': '5.7.0', 'file_path': DATA_MODEL_PATH,
    })


def test_stage_two_refuses_to_consume_after_failures(tmp_path, data_model, monkeypatch):
    db_path, parquet = _staged_run(tmp_path, data_model, failures=2)
    _point_config_at(monkeypatch, db_path)

    with pytest.raises(SystemExit) as excinfo:
        materialize(consume=True)

    assert '2 DQ failure' in str(excinfo.value)
    # nothing touched: the submission survives and so does the view
    assert os.path.exists(parquet)
    con = duckdb.connect(db_path)
    assert get_relation_type(con, TABLE) == 'VIEW'
    con.close()


def test_stage_two_consumes_after_failures_when_forced(tmp_path, data_model, monkeypatch):
    db_path, parquet = _staged_run(tmp_path, data_model, failures=2)
    _point_config_at(monkeypatch, db_path)

    summary = materialize(consume=True, force=True)

    assert summary['removed'] == [parquet]
    assert not os.path.exists(parquet)


def test_stage_two_materializes_without_consuming(tmp_path, data_model, monkeypatch):
    """--force only bears on deleting; materializing after failures is always allowed."""
    db_path, parquet = _staged_run(tmp_path, data_model, failures=2)
    _point_config_at(monkeypatch, db_path)

    summary = materialize(consume=False)

    assert summary['rows'] == 1
    assert summary['removed'] == []
    assert os.path.exists(parquet)
    con = duckdb.connect(db_path)
    assert get_relation_type(con, TABLE) == 'BASE TABLE'
    con.close()


def test_stage_two_warns_rather_than_fails_with_no_views(tmp_path, data_model, monkeypatch):
    db_path, _ = _staged_run(tmp_path, data_model)
    _point_config_at(monkeypatch, db_path)
    materialize(consume=False)

    summary = materialize(consume=False)

    assert summary['tables'] == []


def test_arg_parsing():
    assert parse_args([]).consume is False
    assert parse_args(['--consume']).consume is True
    assert parse_args(['--consume', '--force']).force is True
    assert parse_args(['--run-id', 'abc']).run_id == 'abc'
