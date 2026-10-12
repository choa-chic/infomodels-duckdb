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


# --- redaction of what goes into the shipped duckdb file -----------------------------------

from src.config import (  # noqa: E402
    PERSISTED_CONFIG_KEYS,
    REDACTED,
    redact_config_for_persistence,
)

# Synthetic. Every value here is made up for the test; the point is that none of the
# sensitive-looking ones may survive into the persisted form.
_SITE_CONFIG = {
    'data-models': {
        'mode': 'json',
        'name': 'pedsnet',
        'version': '5.9.0',
        'file_path': '/srv/etl/hostname-01/models/pedsnet_v59_data_model.json',
        'service_base_url': 'https://svc-user:svc-pass@models.example.org:8443/schemata?sig=SASTOKEN123#frag',
    },
    'submission_files': {
        'dir': '/mnt/phi-export/site_acme/2026-10',
        'file_format': 'parquet',
        'multiple_file_per_table': True,
        'access_mode': 'pointer',
        'materialize': 'consume',
        'consume_with_dq_failures': False,
    },
    'duckdb': {
        'path': '/result/site_acme_cdm.duckdb',
        'skip_load': ['measurement*', 'note'],
        'copy_options': 'FORMAT PARQUET',
        'memory_limit': '32GB',
    },
    'core': {
        'log_level': 'INFO',
        'log_path': '/var/log/infomodels/site_acme.log',
        'run_id': 'run-001',
    },
    # keys no version of this tool reads, of the kind a site might leave in the same file
    'warehouse': {
        'connection_string': 'postgresql://dba:hunter2@db.internal.example.org/cdm',
        'password': 'hunter2',
    },
}

_SENSITIVE_FRAGMENTS = (
    'hostname-01', '/srv/etl', '/mnt/phi-export', 'site_acme', '/var/log', '/result',
    'svc-user', 'svc-pass', 'SASTOKEN123', 'frag',
    'hunter2', 'dba', 'db.internal.example.org', 'postgresql://',
)


def test_redacted_config_contains_no_sensitive_fragment():
    persisted = str(redact_config_for_persistence(_SITE_CONFIG))
    for fragment in _SENSITIVE_FRAGMENTS:
        assert fragment not in persisted, fragment


def test_redacted_config_keeps_what_dq_provenance_needs():
    redacted = redact_config_for_persistence(_SITE_CONFIG)
    assert redacted['data-models']['name'] == 'pedsnet'
    assert redacted['data-models']['version'] == '5.9.0'
    assert redacted['data-models']['mode'] == 'json'
    assert redacted['submission_files']['file_format'] == 'parquet'
    assert redacted['submission_files']['multiple_file_per_table'] is True
    assert redacted['submission_files']['access_mode'] == 'pointer'
    assert redacted['submission_files']['materialize'] == 'consume'
    assert redacted['submission_files']['consume_with_dq_failures'] is False
    assert redacted['duckdb']['skip_load'] == ['measurement*', 'note']
    assert redacted['duckdb']['copy_options'] == 'FORMAT PARQUET'
    assert redacted['duckdb']['memory_limit'] == '32GB'
    assert redacted['core']['log_level'] == 'INFO'
    assert redacted['core']['run_id'] == 'run-001'


def test_redacted_config_masks_paths_but_keeps_the_key():
    """A reader should see that the setting was present, not what it held."""
    redacted = redact_config_for_persistence(_SITE_CONFIG)
    assert redacted['submission_files']['dir'] == REDACTED
    assert redacted['duckdb']['path'] == REDACTED
    assert redacted['core']['log_path'] == REDACTED


def test_redacted_config_keeps_only_the_basename_of_the_data_model_file():
    redacted = redact_config_for_persistence(_SITE_CONFIG)
    assert redacted['data-models']['file_path'] == 'pedsnet_v59_data_model.json'


def test_redacted_config_handles_windows_data_model_path():
    redacted = redact_config_for_persistence({'data-models': {'file_path': r'C:\Users\etl\models\pedsnet.json'}})
    assert redacted['data-models']['file_path'] == 'pedsnet.json'


def test_redacted_config_strips_credentials_and_query_from_service_url():
    redacted = redact_config_for_persistence(_SITE_CONFIG)
    assert redacted['data-models']['service_base_url'] == 'https://models.example.org:8443/schemata'


def test_redacted_config_masks_a_service_url_it_cannot_parse():
    redacted = redact_config_for_persistence({'data-models': {'service_base_url': 'not a url'}})
    assert redacted['data-models']['service_base_url'] == REDACTED


def test_redacted_config_masks_unknown_sections_whole():
    """An unknown section's key names are not known to be harmless either, so none survive."""
    redacted = redact_config_for_persistence(_SITE_CONFIG)
    assert redacted['warehouse'] == REDACTED
    assert 'connection_string' not in str(redacted)


def test_redacted_config_masks_unknown_keys_in_a_known_section():
    redacted = redact_config_for_persistence({'duckdb': {'path': '/x', 'token': 'abc', 'skip_load': []}})
    assert redacted['duckdb'] == {'path': REDACTED, 'token': REDACTED, 'skip_load': []}


def test_redacted_config_masks_a_section_that_is_not_a_mapping():
    redacted = redact_config_for_persistence({'duckdb': 'postgresql://u:p@h/db'})
    assert redacted['duckdb'] == REDACTED


def test_redacted_config_does_not_modify_its_input():
    import copy
    original = copy.deepcopy(_SITE_CONFIG)
    redact_config_for_persistence(_SITE_CONFIG)
    assert _SITE_CONFIG == original


def test_redacted_config_tolerates_non_dict_input():
    assert redact_config_for_persistence(None) == {}
    assert redact_config_for_persistence({}) == {}


def test_allowlist_names_no_path_or_credential_keys():
    """Guard the allowlist itself: a future edit must not quietly re-admit these."""
    for section, keys in PERSISTED_CONFIG_KEYS.items():
        for key in keys:
            assert key not in ('dir', 'path', 'log_path'), (section, key)
            for word in ('password', 'secret', 'token', 'key', 'connection'):
                assert word not in key.lower(), (section, key)


def test_allowlist_submission_keys_are_ones_this_version_reads():
    """Persisting a setting the code does not read would record provenance it cannot vouch for."""
    assert PERSISTED_CONFIG_KEYS['submission_files'] <= KNOWN_SUBMISSION_FILE_SETTINGS
