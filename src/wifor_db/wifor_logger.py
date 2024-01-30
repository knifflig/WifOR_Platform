# pylint: disable=line-too-long
"""
Wifor logger
"""
import os
import logging
from wifor_db import _env_cache

def open_log(class_name):
    """
    Sets up a logger that writes to a file named after the class. 
    The log file is stored in a 'log_files' directory.

    Args:
        class_name (str): name of the class for which the logger is being set up.

    Returns:
        logging.Logger: Configured logger object. Returns None if an error occurs during setup.
    """

    log_dir = _env_cache['LOG_DIR']
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_path = os.path.join(log_dir, class_name + ".log")

    logger = logging.getLogger(class_name)
    logger.setLevel(logging.INFO)

    # Check if the logger already has handlers to avoid duplicate logs
    if not logger.handlers:
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def close_log(logger):
    """
    Closes the logger by removing and closing all its handlers and logs a message before closing.

    Args:
        logger (logging.Logger): The logger to be closed.
    """
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)

if __name__ == '__main__':
    try:
        log = open_log("REGIONS")
        log.info(log.name)
        close_log(log)

    except RuntimeError as e:
        print("Error:", e)
