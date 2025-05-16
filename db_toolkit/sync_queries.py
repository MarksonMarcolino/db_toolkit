import pandas as pd
import logging
from psycopg2 import sql
from db_toolkit.db_connection import get_connection, release_connection

# Configura logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Pode ajustar para INFO em produção
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def run_query(host, port, dbname, user, password, query: str, params: tuple = None, verbose: bool = False):
    """
    Execute a single SQL query (synchronously) and return the results as a DataFrame.

    Parameters
    ----------
    host : str
        Database host.
    port : int
        Database port.
    dbname : str
        Database name.
    user : str
        Username for authentication.
    password : str
        Password for authentication.
    query : str
        The SQL query string. Can include `%s` placeholders.
    params : tuple, optional
        Parameters to be passed safely to the query using placeholders.
    verbose : bool
        If True, prints debug logs (query, params, row count, etc.)

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the result of the query.
    """
    if verbose:
        logger.debug(f"Connecting to {host}:{port} database '{dbname}' as user '{user}'")
    
    conn = get_connection(host, port, dbname, user, password)
    
    try:
        with conn.cursor() as cur:
            if verbose:
                logger.debug("Executing query:")
                logger.debug(query)
                if params:
                    logger.debug(f"With params: {params}")

            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            if verbose:
                logger.debug(f"Query returned {len(rows)} rows")

            return pd.DataFrame(rows, columns=columns)
    finally:
        release_connection(conn)
        if verbose:
            logger.debug("Connection released.")