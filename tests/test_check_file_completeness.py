from src.dq_checks.check_file_completeness import check_extra_submission_file, check_missing_submission_file

file_dir = 'tests/data/cdm/base'

def test_check_missing_file():
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('care_site', 'person', 'visit_occurrence'))
    assert result.status == 'PASS'
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('person1', 'person', 'visit_occurrence'))
    assert result.status == 'FAIL'
    assert type(result.table_name) == tuple
    assert len(result.table_name) == 1
    assert result.table_name[0] == 'person1' 
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('person1', 'person2', 'person', 'visit_occurrence'))
    assert result.status == 'FAIL'
    assert type(result.table_name) == tuple
    assert len(result.table_name) == 2
    assert set(result.table_name) == set(['person1', 'person2'])

def test_check_extra_file():
    result = check_extra_submission_file(file_dir, cdm_tables_expected=('person', ))
    assert result.status == 'WARN'
    assert set(result.file_name) == set(['observation.csv', 'visit_occurrence.csv', 'location.csv', 'provider.csv', 'observation_period.csv', 'drug_exposure.csv', 'condition_occurrence.csv', 'fact_relationship.csv', 'procedure_occurrence.csv', 'care_site.csv'])

# --- extra files, on synthetic directories --------------------------------------------------
# The extra-file check used to subtract the on-disk set from itself, so it could never fire.

def _touch(directory, *names):
    for name in names:
        (directory / name).write_text('')


def test_extra_csv_file_is_reported_by_its_file_name(tmp_path):
    _touch(tmp_path, 'person.csv', 'visit_occurrence.csv', 'not_a_cdm_table.csv')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person', 'visit_occurrence'))
    assert result.status == 'WARN'
    assert result.file_name == ('not_a_cdm_table.csv',)


def test_no_extra_file_passes(tmp_path):
    _touch(tmp_path, 'person.csv', 'visit_occurrence.csv')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person', 'visit_occurrence', 'care_site'))
    assert result.status == 'PASS'
    assert result.file_name is None


def test_several_extra_files_are_all_reported_sorted(tmp_path):
    _touch(tmp_path, 'person.csv', 'zzz.csv', 'aaa.csv')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person',))
    assert result.status == 'WARN'
    assert result.file_name == ('aaa.csv', 'zzz.csv')


def test_extra_parquet_file_uses_the_parquet_extension(tmp_path):
    _touch(tmp_path, 'person.parquet', 'extra.parquet')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person',), file_format='parquet')
    assert result.status == 'WARN'
    assert result.file_name == ('extra.parquet',)


def test_files_of_another_format_are_not_extra_files(tmp_path):
    """Only files the loader would consider count; a stray README is not a misnamed table."""
    _touch(tmp_path, 'person.csv', 'README.txt', 'notes.parquet')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person',))
    assert result.status == 'PASS'


def test_extra_table_directory_is_reported_by_directory_name(tmp_path):
    (tmp_path / 'person').mkdir()
    (tmp_path / 'mystery_table').mkdir()
    _touch(tmp_path / 'person', 'part-0.parquet')
    _touch(tmp_path / 'mystery_table', 'part-0.parquet')
    _touch(tmp_path, 'loose.parquet')  # a loose file is not a table in this layout
    result = check_extra_submission_file(
        str(tmp_path), cdm_tables_expected=('person',), file_format='parquet', multiple_file_per_table=True
    )
    assert result.status == 'WARN'
    assert result.file_name == ('mystery_table',)


def test_extra_check_is_skipped_without_expected_tables(tmp_path):
    _touch(tmp_path, 'person.csv')
    result = check_extra_submission_file(str(tmp_path), cdm_tables_expected=())
    assert result.status == 'SKIPPED'


def test_extra_and_missing_checks_are_mirror_images(tmp_path):
    _touch(tmp_path, 'person.csv', 'surprise.csv')
    missing = check_missing_submission_file(str(tmp_path), cdm_tables_expected=('person', 'care_site'))
    extra = check_extra_submission_file(str(tmp_path), cdm_tables_expected=('person', 'care_site'))
    assert missing.status == 'FAIL' and missing.table_name == ('care_site',)
    assert extra.status == 'WARN' and extra.file_name == ('surprise.csv',)
