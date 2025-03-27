import os
from dotenv import load_dotenv
from psycopg2 import sql
from datetime import datetime

def load_env(dotenv_path=None):
    """
    Load environment variables from a `.env` file.

    If no path is provided, it defaults to loading a `.env` file
    located one level above the current module.

    Parameters
    ----------
    dotenv_path : str or None, optional
        Custom path to the `.env` file. If None, a default path is used.
    """
    if dotenv_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dotenv_path = os.path.join(current_dir, '..', '.env')

    load_dotenv(dotenv_path)

def get_env_variable(key, default=None):
    """
    Get the value of an environment variable.

    Parameters
    ----------
    key : str
        The name of the environment variable to retrieve.
    default : str or None, optional
        A default value to return if the environment variable is not found.

    Returns
    -------
    str or None
        The value of the environment variable, or the default if not found.
    """
    return os.getenv(key, default)

def require_env(keys):
    """
    Ensure that all required environment variables are set.

    Parameters
    ----------
    keys : list of str
        A list of environment variable names to validate.

    Raises
    ------
    EnvironmentError
        If one or more environment variables are not set.
    """
    missing = [key for key in keys if not os.getenv(key)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
    
def safe_identifier(name):
    """
    Creates a psycopg2.sql.Identifier from a string or tuple (e.g., for schema.table).

    Parameters
    ----------
    name : str or tuple of str
        A single identifier (e.g., 'table') or a qualified identifier (e.g., ('schema', 'table')).

    Returns
    -------
    psycopg2.sql.Identifier
        A safely quoted SQL identifier.
    """
    if isinstance(name, tuple):
        if not all(isinstance(part, str) for part in name):
            raise TypeError("All parts of the identifier tuple must be strings")
        return sql.Identifier(*name)
    elif isinstance(name, str):
        return sql.Identifier(name)
    else:
        raise TypeError("Identifier must be a string or tuple of strings")

def log_query_failure(values, error_msg, log_file="query_failures.log"):
    """
    Log the final failure of a query after all retries have been exhausted.

    Parameters
    ----------
    values : tuple
        The parameter values used in the failed query.
    error_msg : str
        The error message received from the exception.
    log_file : str, optional
        The file path to write the log to. Defaults to 'query_failures.log'.
    """
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] VALUES: {values} | ERROR: {error_msg}\n")

def log_query_retry(values, attempt, error_msg, log_file="query_retries.log"):
    """
    Log an intermediate retry attempt for a query that failed.

    Parameters
    ----------
    values : tuple
        The parameter values used in the query.
    attempt : int
        The current retry attempt number.
    error_msg : str
        The error message received from the exception.
    log_file : str, optional
        The file path to write the log to. Defaults to 'query_retries.log'.
    """
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] RETRY {attempt} for VALUES: {values} | ERROR: {error_msg}\n")