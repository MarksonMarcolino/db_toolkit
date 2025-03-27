import pandas as pd
from psycopg2 import sql
from db_toolkit.db_connection import get_connection, release_connection

def run_query(host, port, dbname, user, password, query: str, params: tuple = None):
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

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the result of the query.
    """
    # Get a connection from the pool
    conn = get_connection(host, port, dbname, user, password)
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
    finally:
        # Release the connection back to the pool
        release_connection(conn)