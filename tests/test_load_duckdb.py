import duckdb
import pytest

from src.config import CONFIG
from src.data_model import DataModel
from src.load_duckdb import (
    build_parquet_source_sql,
    create_duckdb_tables,
    create_parquet_pointer_view,
    drop_relation_if_exists,
    load_parquet_to_duckdb,
)

DATA_MODEL_PATH = 'tests/data/data_model/pedsnet_v57_data_model.json'
TABLE = 'care_site'


@pytest.fixture
def parquet_copy_options(monkeypatch):
    """load_parquet_to_duckdb() reads copy_options from the global CONFIG."""
    monkeypatch.setitem(CONFIG['duckdb'], 'copy_options', 'FORMAT PARQUET')


@pytest.fixture
def data_model():
    return DataModel(mode='json', name='pedsnet', version='5.7.0', file_path=DATA_MODEL_PATH)


@pytest.fixture
def con(data_model):
    connection = duckdb.connect()
    create_duckdb_tables(data_model, connection, recreate=True)
    yield connection
    connection.close()


def _write_parquet(tmp_path, name, select_sql):
    """Write one parquet part from a SELECT and return its path."""
    path = tmp_path / name
    with duckdb.connect() as writer:
        writer.execute(f"COPY ({select_sql}) TO '{path}' (FORMAT PARQUET)")
    return str(path)


def _relation_type(con, table_name):
    return con.execute(
        "SELECT table_type FROM information_schema.tables WHERE lower(table_name) = lower(?)",
        (table_name,)
    ).fetchone()[0]


def _columns(con, table_name):
    return {row[0]: row[1] for row in con.execute(f'DESCRIBE "{table_name}"').fetchall()}


def test_build_parquet_source_sql_unions_by_name():
    sql = build_parquet_source_sql(['a.parquet', 'b.parquet'])
    assert 'union_by_name = true' in sql
    assert "'a.parquet'" in sql and "'b.parquet'" in sql


def test_build_parquet_source_sql_accepts_single_path():
    assert build_parquet_source_sql('a.parquet') == build_parquet_source_sql(['a.parquet'])


def test_build_parquet_source_sql_rejects_empty():
    with pytest.raises(ValueError):
        build_parquet_source_sql([])


def test_pointer_view_is_a_view_not_a_table(tmp_path, con):
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id, 'clinic' AS care_site_name")
    create_parquet_pointer_view(parquet_path=path, con=con, table_name=TABLE)

    assert _relation_type(con, TABLE) == 'VIEW'
    assert con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0] == 1


def test_pointer_view_has_same_columns_and_types_as_copy_mode(tmp_path, data_model, parquet_copy_options):
    """A pointer view must be a drop-in for a copied table, or the checks diverge."""
    select_sql = "SELECT 1 AS care_site_id, 'clinic' AS care_site_name, 7 AS location_id"
    path = _write_parquet(tmp_path, 'care_site.parquet', select_sql)

    copied = duckdb.connect()
    create_duckdb_tables(data_model, copied, recreate=True)
    load_parquet_to_duckdb(parquet_path=path, con=copied, table_name=TABLE)

    pointed = duckdb.connect()
    create_duckdb_tables(data_model, pointed, recreate=True)
    create_parquet_pointer_view(parquet_path=path, con=pointed, table_name=TABLE)

    assert _columns(pointed, TABLE) == _columns(copied, TABLE)
    assert (con_rows(pointed) == con_rows(copied))
    copied.close()
    pointed.close()


def con_rows(connection):
    cols = ', '.join(sorted(_columns(connection, TABLE)))
    return connection.execute(f'SELECT {cols} FROM "{TABLE}"').fetchall()


def test_pointer_view_aligns_parts_by_name_not_position(tmp_path, con):
    """Parts written with different column orders must not shift values between columns."""
    part_1 = _write_parquet(tmp_path, 'part_1.parquet', "SELECT 1 AS care_site_id, 'first' AS care_site_name")
    part_2 = _write_parquet(tmp_path, 'part_2.parquet', "SELECT 'second' AS care_site_name, 2 AS care_site_id")

    create_parquet_pointer_view(parquet_path=[part_1, part_2], con=con, table_name=TABLE)

    rows = con.execute(f'SELECT care_site_id, care_site_name FROM "{TABLE}" ORDER BY care_site_id').fetchall()
    assert rows == [(1, 'first'), (2, 'second')]


def test_pointer_view_lowercases_and_casts_to_model_types(tmp_path, con):
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT '1' AS CARE_SITE_ID, 'clinic' AS Care_Site_Name")
    create_parquet_pointer_view(parquet_path=path, con=con, table_name=TABLE)

    columns = _columns(con, TABLE)
    assert columns['care_site_id'] == 'BIGINT'          # cast from the parquet's VARCHAR
    assert con.execute(f'SELECT care_site_id FROM "{TABLE}"').fetchone()[0] == 1


def test_pointer_view_exposes_columns_the_model_omits(tmp_path, con):
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id, 'x' AS site_extra_col")
    create_parquet_pointer_view(parquet_path=path, con=con, table_name=TABLE, accept_additional_col=True)

    assert _columns(con, TABLE)['site_extra_col'] == 'VARCHAR'


def test_pointer_view_rejects_extra_columns_when_asked(tmp_path, con):
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id, 'x' AS site_extra_col")
    with pytest.raises(ValueError):
        create_parquet_pointer_view(parquet_path=path, con=con, table_name=TABLE, accept_additional_col=False)


def test_pointer_view_fills_columns_the_file_omits(tmp_path, con):
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id")
    create_parquet_pointer_view(parquet_path=path, con=con, table_name=TABLE)

    # location_id is in the data model but not the file; the checks still expect the column
    assert 'location_id' in _columns(con, TABLE)
    assert con.execute(f'SELECT location_id FROM "{TABLE}"').fetchone()[0] is None


def test_runs_can_alternate_between_copy_and_pointer(tmp_path, data_model, parquet_copy_options):
    """A duckdb file left holding views must not break the next run's CREATE TABLE."""
    path = _write_parquet(tmp_path, 'care_site.parquet', "SELECT 1 AS care_site_id")
    db_path = str(tmp_path / 'cdm.duckdb')

    with duckdb.connect(db_path) as first:
        create_duckdb_tables(data_model, first, recreate=True)
        create_parquet_pointer_view(parquet_path=path, con=first, table_name=TABLE)

    with duckdb.connect(db_path) as second:
        create_duckdb_tables(data_model, second, recreate=True)   # would raise before drop_relation_if_exists
        assert _relation_type(second, TABLE) == 'BASE TABLE'
        load_parquet_to_duckdb(parquet_path=path, con=second, table_name=TABLE)
        assert second.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0] == 1


def test_drop_relation_if_exists_is_a_noop_when_absent(con):
    drop_relation_if_exists(con, 'no_such_table')
