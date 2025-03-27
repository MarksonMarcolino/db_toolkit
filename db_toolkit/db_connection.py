import psycopg2
from psycopg2 import pool

# Global connection pool
connection_pool = None

def create_connection_pool(host, port, dbname, user, password, minconn=1, maxconn=10):
    """
    Initialize a global PostgreSQL connection pool.

    Parameters
    ----------
    host : str
        The database host address.
    port : int
        The port number to connect to.
    dbname : str
        The name of the database.
    user : str
        Username used to authenticate.
    password : str
        Password used to authenticate.
    minconn : int, default=1
        Minimum number of connections to maintain in the pool.
    maxconn : int, default=10
        Maximum number of connections to maintain in the pool.

    Returns
    -------
    None
        Initializes the global connection pool for future use.
    """
    global connection_pool
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=minconn,
        maxconn=maxconn,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    if connection_pool:
        print("[INFO] Connection pool created successfully.")


def get_connection(host=None, port=None, dbname=None, user=None, password=None):
    """
    Retrieve a PostgreSQL connection from the pool or create a direct connection.

    If all parameters are provided, creates a direct connection. Otherwise,
    retrieves a connection from the global connection pool.

    Parameters
    ----------
    host : str, optional
        Database host address.
    port : int, optional
        Port number to connect to.
    dbname : str, optional
        Name of the database.
    user : str, optional
        Username used to authenticate.
    password : str, optional
        Password used to authenticate.

    Returns
    -------
    psycopg2.extensions.connection
        A valid PostgreSQL connection object.

    Raises
    ------
    Exception
        If the connection pool is not initialized and parameters are not provided.
    """
    if all([host, port, dbname, user, password]):
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
    elif connection_pool:
        return connection_pool.getconn()
    else:
        raise Exception("Connection pool is not initialized and no parameters were given.")


def release_connection(conn):
    """
    Return a connection to the global pool.

    Parameters
    ----------
    conn : psycopg2.extensions.connection
        The PostgreSQL connection object to be returned.

    Returns
    -------
    None
    """
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            print(f"[WARNING] Could not return connection to pool: {e}")


def close_pool():
    """
    Close all connections in the global connection pool.

    Returns
    -------
    None
    """
    if connection_pool:
        connection_pool.closeall()
        print("All connections in the pool have been closed.")