import os
import pathlib
import tempfile

import yaml

# src.config loads its yaml at import time, so a usable config has to exist before any
# test module imports from src. Writing it outside the repo keeps a developer's own
# config.yml (and the placeholder one in git) out of the way of the tests.
_TEST_CONFIG = {
    'data-models': {
        'mode': 'json',
        'name': 'pedsnet',
        'version': '5.7.0',
        'file_path': 'tests/data/data_model/pedsnet_v57_data_model.json',
    },
    'submission_files': {
        'dir': 'tests/data/cdm/base',
        'file_format': 'csv',
        'multiple_file_per_table': False,
        'access_mode': 'copy',
    },
    'duckdb': {
        'path': ':memory:',
        'skip_load': [],
        'copy_options': "FORMAT CSV, HEADER, DELIM ',', ESCAPE '\"'",
    },
    'core': {'log_level': 'ERROR'},
}

_config_dir = pathlib.Path(tempfile.mkdtemp(prefix='infomodels_test_'))
_TEST_CONFIG['core']['log_path'] = str(_config_dir / 'test.log')
_config_path = _config_dir / 'config.yml'
_config_path.write_text(yaml.safe_dump(_TEST_CONFIG))

os.environ.setdefault('INFOMODELS_CONFIG', str(_config_path))
