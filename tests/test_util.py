import duckdb

from src.util import column_exists, get_threshold, table_exists


def test_get_threshold():
    assert get_threshold(check_type='foreign_key_violation', table_name='person', column_name = 'person_id') == 0.01
    assert get_threshold(check_type='foreign_key_violation', table_name='visit_occurrence_id', column_name = 'some_other_column') == 0.05

def test_table_exists_with_schema():
    """The schema branch used to send an f-string literal to duckdb and always raise."""
    con = duckdb.connect()
    con.execute('CREATE SCHEMA logging; CREATE TABLE logging.dq (run_id VARCHAR);')

    assert table_exists(con, 'dq', schema='logging') is True
    assert table_exists(con, 'no_such_table', schema='logging') is False
    con.close()


def test_column_exists_with_schema():
    con = duckdb.connect()
    con.execute('CREATE SCHEMA logging; CREATE TABLE logging.dq (run_id VARCHAR);')

    assert column_exists(con, 'dq', 'run_id', schema='logging') is True
    assert column_exists(con, 'dq', 'nope', schema='logging') is False
    con.close()
