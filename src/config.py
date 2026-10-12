import yaml
import logging
import os
import posixpath
import sys
from urllib.parse import urlsplit, urlunsplit

# INFOMODELS_CONFIG lets a run (or the test suite) point at a config outside the cwd
CONFIG_FILE = os.environ.get('INFOMODELS_CONFIG', 'config.yml')
CONFIG_FILE_PATH = os.path.abspath(CONFIG_FILE)
CONFIG_FILE_FROM_ENV = 'INFOMODELS_CONFIG' in os.environ

with open(CONFIG_FILE, "r") as f:
    CONFIG = yaml.safe_load(f)

# The settings under submission_files that this version reads. Anything else is inert:
# a config asking for behaviour the code predates is loaded, logged in the config dump,
# and then silently ignored, which reads exactly like the behaviour was applied.
KNOWN_SUBMISSION_FILE_SETTINGS = frozenset({
    'dir',
    'file_format',
    'multiple_file_per_table',
    'access_mode',
    'materialize',
    'consume_with_dq_failures',
})


# What a run writes into logging.run.config inside the duckdb file. That file is the
# deliverable: it leaves the site, so anything persisted into it is disclosed to whoever
# receives it. The config.yml it comes from is written by site staff and is not under this
# code's control -- it can hold paths that describe the host, and nothing stops a site from
# keeping a connection string or token next to the settings this tool reads.
#
# This is an ALLOWLIST, not a denylist: the config has a fixed shape (four sections, each
# with a known set of keys), so the safe keys can be named outright, and a key this code has
# never heard of is masked rather than guessed at. A denylist would need to anticipate every
# spelling of "password", and would pass a new secret-bearing key straight through.
#
# Kept: what someone reading the DQ results needs to interpret them -- which data model and
# version the checks ran against, how the submission was read (format, layout, access mode,
# what happened to the files afterwards), which tables were deliberately not loaded, and the
# duckdb options that shaped the load. Masked: every filesystem path, the log file, and
# anything not in this list. The masked value keeps the key name, so a reader can see a
# setting was present without seeing what it held.
PERSISTED_CONFIG_KEYS = {
    'data-models': frozenset({'mode', 'name', 'version', 'service_base_url', 'file_path'}),
    'submission_files': frozenset({
        'file_format', 'multiple_file_per_table', 'access_mode', 'materialize',
        'consume_with_dq_failures',
    }),
    'duckdb': frozenset({'skip_load', 'copy_options', 'memory_limit'}),
    'core': frozenset({'log_level', 'run_id'}),
}
# Allowlisted keys whose value is a local path: only the final component is kept. The
# basename of a data model file (pedsnet_v57_data_model.json) is provenance; the directory
# that held it is host layout.
PERSISTED_CONFIG_BASENAME_KEYS = frozenset({('data-models', 'file_path')})
# Allowlisted keys whose value is a URL: userinfo, query string and fragment are dropped, so
# a SAS token, signature or embedded credential never reaches the file. Scheme, host and
# path stay, which is what identifies the data model service.
PERSISTED_CONFIG_URL_KEYS = frozenset({('data-models', 'service_base_url')})
REDACTED = '<redacted>'


def _scrub_url(value):
    if not isinstance(value, str):
        return REDACTED
    parts = urlsplit(value)
    if not parts.scheme or not parts.netloc:
        # not something urlsplit can take apart safely, so do not pretend it is clean
        return REDACTED
    host = parts.hostname or ''
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, '', ''))


def _scrub_path(value):
    if not isinstance(value, str):
        return REDACTED
    # posixpath on a normalised separator handles both Windows and POSIX paths the same way
    basename = posixpath.basename(value.replace('\\', '/').rstrip('/'))
    return basename if basename else REDACTED


def redact_config_for_persistence(run_config) -> dict:
    """
    Return the subset of the run config that is safe to write into the shipped duckdb file.

    See PERSISTED_CONFIG_KEYS for what is kept and why. The result keeps the shape of the
    input -- the same sections and keys appear -- but every value that is not allowlisted is
    replaced with REDACTED, so a reader can tell a setting was configured without learning
    what it was set to. Sections this code does not know are masked whole, without
    descending into them, since their key names are not known to be harmless either.

    The input is not modified.
    """
    if not isinstance(run_config, dict):
        return {}
    redacted = {}
    for section, settings in run_config.items():
        allowed = PERSISTED_CONFIG_KEYS.get(section)
        if allowed is None or not isinstance(settings, dict):
            redacted[section] = REDACTED
            continue
        kept = {}
        for key, value in settings.items():
            if key not in allowed:
                kept[key] = REDACTED
            elif (section, key) in PERSISTED_CONFIG_URL_KEYS:
                kept[key] = _scrub_url(value)
            elif (section, key) in PERSISTED_CONFIG_BASENAME_KEYS:
                kept[key] = _scrub_path(value)
            else:
                kept[key] = value
        redacted[section] = kept
    return redacted


def get_logger(name='main') -> logging.Logger:
    # Create a global logger instance
    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName(CONFIG['core']['log_level']))
    # Add DQ result level
    logging.addLevelName(60, "DQ")
    logging.Logger.DQ = lambda self, message, *args, **kwargs: self._log(60, message, args, **kwargs)
    # Add console handler and file handler
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.getLevelName(CONFIG['core']['log_level']))
        # File handler
        file_handler = logging.FileHandler(CONFIG['core']['log_path'])
        file_handler.setLevel(logging.getLevelName(CONFIG['core']['log_level']))
        # Formatter
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s', 
                                    datefmt='%Y-%m-%d %H:%M:%S')
        # Attach formatter to handlers
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        # Add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    # Define a custom exception hook for uncaught exception from system in logger
    def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Call the default excepthook for KeyboardInterrupt
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        # Log the error
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    # Reroute system exception to logger
    sys.excepthook = log_uncaught_exceptions
    return logger


LOGGER = get_logger()
# Say where the settings came from. An env var can redirect this to a file nobody edited,
# and a config dump alone cannot tell you which file produced it.
LOGGER.info(
    f"Config loaded from {CONFIG_FILE_PATH}"
    + (" (path taken from INFOMODELS_CONFIG)" if CONFIG_FILE_FROM_ENV else "")
)


def warn_unrecognized_settings(logger: logging.Logger = None) -> list:
    """
    Warn about submission_files settings this version does not read.

    Running a config against a build that predates one of its settings is silent today:
    the key appears in the config dump and is then ignored, so a run asked for one mode
    and performed another with nothing in the log to say so. Naming the ignored keys at
    startup makes that visible immediately rather than at the end of a long run.

    Returns the sorted list of unrecognized setting names.
    """
    logger = logger or LOGGER
    unrecognized = sorted(set(CONFIG.get('submission_files', {})) - KNOWN_SUBMISSION_FILE_SETTINGS)
    if unrecognized:
        logger.warning(
            f"Ignoring submission_files setting(s) this version does not support: {unrecognized}. "
            "They have no effect on this run. Check the spelling, and that this build is the one "
            "you meant to run."
        )
    return unrecognized