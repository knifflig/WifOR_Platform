# pylint: disable=line-too-long
"""
This script is designed to facilitate the management of file directories and environment variables in Python projects. 
It is particularly useful in environments where the script's location or the project's base directory needs to be dynamically determined, 
such as in Jupyter notebooks or when working with different deployment environments. 

Functions:
    get_file_dir():
        Determines and returns the base directory of the current process. It checks if the script is running in a Jupyter 
        notebook and returns the current working directory, or if running as a standalone script, it returns the directory 
        of the script. Includes comprehensive error handling to cover various edge cases.

    get_base_dir(current_dir, file_name="poetry.lock"):
        Traverses up from the current directory to find a base directory identified by the presence of a specific file 
        (default is 'poetry.lock'). This function is useful in projects where a certain file marks the root of the 
        project. It includes error handling for cases where the file is not found or other issues arise during directory traversal.

    get_env():
        Loads environment variables from a .env file and computes relevant paths including the current directory, base directory, 
        class directory, and log directory. This function is vital for projects that rely on environment variables for configuration. 
        It includes error handling for missing environment variables and other unexpected issues.

The script emphasizes robust error handling, ensuring that exceptions are caught and handled appropriately, providing clear error messages. 
This makes the script suitable for use in a variety of environments and projects, enhancing its reliability and ease of integration.

Note:
    This script is intended for use in Python projects that require dynamic path management and environment variable loading. 
    It is not specific to any particular project structure, making it versatile and adaptable.
"""

import os
import sys
from dotenv import load_dotenv

# Global variable for caching environment variables and paths
_env_cache = {}

def get_file_dir():
    """
    Determines the base directory of the current process.
    Returns the directory where the Jupyter Notebook is located if running in Jupyter,
    or the directory of the script file if running as a standalone script.
    Includes extended error handling.
    
    :return: The absolute path of the current directory or script file.
    :raises RuntimeError: If an error occurs while determining the base directory.
    """
    try:
        # Check if the script is running in a Jupyter notebook
        if 'ipykernel' in sys.modules and 'IPython' in sys.modules:
            # Running in Jupyter Notebook
            current_directory = os.getcwd()
            return current_directory
        else:
            # Running as a standalone script
            script_directory = os.path.dirname(os.path.abspath(__file__))
            return script_directory
    except NameError as name_error:
        # Fallback if __file__ is not defined (e.g., Jupyter Notebook, interactive shell)
        try:
            current_directory = os.getcwd()
            return current_directory
        except Exception as general_error:
            raise RuntimeError("Unable to determine the base directory: " + str(general_error)) from name_error
    except Exception as general_error:
        # General exception catch for other unforeseen errors
        raise RuntimeError("An error occurred while determining the base directory: " + str(general_error)) from general_error

def get_base_dir(current_dir, file_name="poetry.lock"):
    """
    Traverse up the directory tree to find the base directory based on the presence of a specific file.
    
    :param current_dir: The starting directory (typically where the script is).
    :param file_name: The name of the file to look for, default is 'poetry.lock'.
    :return: The absolute path to the base directory.
    :raises FileNotFoundError: If the specified file is not found in any parent directories.
    :raises RuntimeError: For other errors that occur during directory traversal.
    """
    try:
        while not os.path.exists(os.path.join(current_dir, file_name)):
            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:
                # We've reached the root directory without finding the file
                raise FileNotFoundError(f"Could not find the {file_name} file to identify the base directory.")
            current_dir = parent_dir
        return current_dir
    except FileNotFoundError as fnf_error:
        # Handle file not found error specifically
        raise FileNotFoundError(f"File not found error: {fnf_error}") from fnf_error
    except Exception as general_error:
        # Handle any other exceptions that may occur
        raise RuntimeError(f"An error occurred while trying to find the base directory: {general_error}") from general_error

def get_env():
    """
    Loads environment variables from .env file and computes relevant paths.
    Stores all environment variables and paths in a global cache.

    Raises:
        RuntimeError: If any error occurs during the loading of environment variables or path computation.
    """
    # Referencing the global env_cache
    # pylint: disable=global-variable-not-assigned
    global _env_cache

    try:
        # Load environment variables from .env file
        load_dotenv()

        # Iterate through all environment variables and store them in the cache
        for key, value in os.environ.items():
            _env_cache[key] = value

        # Calling helper functions to get current directory and base directory
        current_directory = get_file_dir()
        base_directory = get_base_dir(current_directory)

        # Extracting specific environment variable values
        class_path = os.environ.get('CLASS_DICT')
        if class_path is None:
            # Raising an error if CLASS_DICT is not set
            raise EnvironmentError("CLASS_DICT environment variable not set")

        # Building the full path for class directory
        class_directory = os.path.join(base_directory, class_path)

        # Extracting the LOG_DICT environment variable
        log_path = os.environ.get('LOG_DICT')
        if log_path is None:
            # Raising an error if LOG_DICT is not set
            raise EnvironmentError("LOG_DICT environment variable not set")

        # Building the full path for log directory
        log_directory = os.path.join(base_directory, log_path)

        # Storing the computed paths in the cache
        _env_cache['CURRENT_DIR'] = current_directory
        _env_cache['BASE_DIR'] = base_directory
        _env_cache['CLASS_DIR'] = class_directory
        _env_cache['LOG_DIR'] = log_directory

    except FileNotFoundError as file_not_found_error:
        # Specific error handling for file not found issues
        raise RuntimeError(f"File not found error: {file_not_found_error}") from file_not_found_error
    except EnvironmentError as environment_error:
        # Specific error handling for environment variable issues
        raise RuntimeError(f"Environment variable error: {environment_error}") from environment_error
    except Exception as general_error:
        # General catch-all for any other exceptions
        raise RuntimeError(f"An unexpected error occurred: {general_error}") from general_error


if __name__ == '__main__':
    try:
        get_env()

    except RuntimeError as e:
        print("Error:", e)
