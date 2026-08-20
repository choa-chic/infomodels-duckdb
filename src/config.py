import yaml
import logging
import os
import sys

# INFOMODELS_CONFIG lets a run (or the test suite) point at a config outside the cwd
CONFIG_FILE = os.environ.get('INFOMODELS_CONFIG', 'config.yml')

with open(CONFIG_FILE, "r") as f:
    CONFIG = yaml.safe_load(f)


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