import logging

from src.config import (
    CONFIG,
    CONFIG_FILE_PATH,
    KNOWN_SUBMISSION_FILE_SETTINGS,
    warn_unrecognized_settings,
)


class _Recorder(logging.Logger):
    """Collects warnings instead of emitting them."""
    def __init__(self):
        super().__init__('recorder')
        self.warnings = []

    def warning(self, message, *args, **kwargs):
        self.warnings.append(message)


def test_known_settings_cover_the_documented_ones():
    for setting in ('dir', 'file_format', 'multiple_file_per_table', 'access_mode'):
        assert setting in KNOWN_SUBMISSION_FILE_SETTINGS


def test_no_warning_when_every_setting_is_recognized(monkeypatch):
    monkeypatch.setitem(CONFIG, 'submission_files', {'dir': '/data', 'access_mode': 'pointer'})
    recorder = _Recorder()

    assert warn_unrecognized_settings(recorder) == []
    assert recorder.warnings == []


def test_warns_about_a_setting_this_version_cannot_read(monkeypatch):
    """The failure this guards: a build that predates a setting ignores it in silence."""
    monkeypatch.setitem(CONFIG, 'submission_files', {'dir': '/data', 'not_a_real_setting': True})
    recorder = _Recorder()

    assert warn_unrecognized_settings(recorder) == ['not_a_real_setting']
    assert 'not_a_real_setting' in recorder.warnings[0]


def test_warns_about_a_misspelled_setting(monkeypatch):
    monkeypatch.setitem(CONFIG, 'submission_files', {'access_modes': 'pointer'})
    recorder = _Recorder()

    assert warn_unrecognized_settings(recorder) == ['access_modes']


def test_tolerates_a_missing_submission_files_block(monkeypatch):
    monkeypatch.delitem(CONFIG, 'submission_files', raising=False)

    assert warn_unrecognized_settings(_Recorder()) == []


def test_config_file_path_is_absolute():
    assert CONFIG_FILE_PATH.startswith('/')


def test_known_settings_cover_the_materialize_ones():
    """This branch reads these; without them a valid config would be reported as ignored."""
    for setting in ('materialize', 'consume_with_dq_failures'):
        assert setting in KNOWN_SUBMISSION_FILE_SETTINGS


def test_a_full_materialize_config_raises_no_warning(monkeypatch):
    monkeypatch.setitem(CONFIG, 'submission_files', {
        'dir': '/data', 'file_format': 'parquet', 'multiple_file_per_table': True,
        'access_mode': 'pointer', 'materialize': 'consume', 'consume_with_dq_failures': False,
    })
    assert warn_unrecognized_settings(_Recorder()) == []
