import yaml
import logging
import os
import sys

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