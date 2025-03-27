from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2 import sql
import pandas as pd
from tqdm import tqdm
from db_toolkit.utils import safe_identifier
import threading
from itertools import product
from db_toolkit.db_connection import get_connection, release_connection, create_connection_pool
from db_toolkit.utils import log_query_retry, log_query_failure
import time
import random

def run_parallel_queries(
    host, port, dbname, user, password,  # Adding connection parameters
    query_template: str,
    target_table,
    distinct_sources: dict,
    verbose: bool = True,
    debug: bool = False,  # New debug parameter
    max_combinations: int = None  # New parameter for max combinations in debug mode
):
    """
    Run SQL queries in parallel using distinct combinations of values from multiple attributes,
    where each attribute may come from a different table.

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
    query_template : str
        SQL query template with `{table}` and `{attribute_0}`, `{attribute_1}`, ... placeholders.
        Must include `%s` for each attribute value in WHERE clause.
    target_table : str or tuple
        Table (or schema, table) used in the query's FROM clause.
    distinct_sources : dict
        Mapping of attribute name to table (or schema, table) to use in the DISTINCT queries.
    verbose : bool, optional
        If True, prints progress and active worker count. Default is True.
    debug : bool, optional
        If True, prints debug information for each query and halts if an error occurs.
    max_combinations : int, optional
        If provided, limits the number of combinations for debugging purposes.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the concatenated results of all parallel queries.
    """
    create_connection_pool(host, port, dbname, user, password)
    attributes = tuple(distinct_sources.keys())

    def fetch_distinct_values():
        """
        Retrieve all distinct combinations of attribute values from source tables.

        Executes a DISTINCT query for each attribute defined in `distinct_sources` to 
        gather unique values, and then builds the Cartesian product of these values 
        to generate all possible combinations.

        Returns
        -------
        list of tuple
            A list of attribute value combinations to be used in parameterized queries.
        """
        values_by_attribute = {}
        conn = get_connection(host, port, dbname, user, password)
        try:
            with conn.cursor() as cur:
                for attr, source_table in distinct_sources.items():
                    if verbose:
                        print(f"[INFO] Fetching distinct values for attribute: {attr}")
                    cur.execute(
                        sql.SQL("SELECT DISTINCT {attr} FROM {tbl}").format(
                            attr=safe_identifier(attr),
                            tbl=safe_identifier(source_table)
                        )
                    )
                    values = [row[0] for row in cur.fetchall()]
                    values_by_attribute[attr] = values
        finally:
            release_connection(conn)

        all_combinations = list(product(*values_by_attribute.values()))
        
        if debug and max_combinations:
            all_combinations = all_combinations[:max_combinations]
        
        return all_combinations

    def execute_query(values, max_retries=3, base_delay=1):
        """
        Execute a single SQL query using a specific combination of attribute values.

        Parameters
        ----------
        values : tuple
            A tuple of values to substitute into the parameterized SQL query.
        max_retries : int, default=3
            Maximum number of retry attempts on failure.
        base_delay : float, default=1
            Initial delay in seconds for retry backoff, exponentially increased.

        Returns
        -------
        pandas.DataFrame or None
            A DataFrame containing the result of the query if successful, otherwise None.
        """
        thread_name = threading.current_thread().name
        if verbose:
            print(f"[{thread_name}] Running for values: {values}")

        for attempt in range(1, max_retries + 1):
            conn = None
            try:
                conn = get_connection()
                
                attr_placeholders = {
                    f"attribute_{i}": attr for i, attr in enumerate(attributes)
                }
                query_str = query_template.format(table="{table}", **attr_placeholders)

                final_query = sql.SQL(query_str).format(
                    table=safe_identifier(target_table),
                    **attr_placeholders
                )

                if debug:
                    print(f"[DEBUG] Attempt {attempt} - Query: {final_query.as_string(conn)}")

                with conn.cursor() as cur:
                    cur.execute(final_query, values)
                    rows = cur.fetchall()

                    if debug:
                        print(f"[DEBUG] Retrieved {len(rows)} rows for {values}")

                    if not rows:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    return pd.DataFrame(rows, columns=columns)

            except Exception as e:
                error_msg = str(e)
                if attempt == max_retries:
                    print(f"[ERROR] Query failed for values {values} after {max_retries} attempts: {e}")
                    log_query_failure(values, error_msg)
                    return None
                else:
                    log_query_retry(values, attempt, error_msg)
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    print(f"[RETRY] Attempt {attempt} failed for values {values}. Retrying in {delay:.2f}s...")
                    time.sleep(delay)

            finally:
                if conn:
                    try:
                        release_connection(conn)
                    except Exception:
                        pass

    distinct_values = fetch_distinct_values()
    total = len(distinct_values)

    if verbose:
        attr_names = ", ".join(attributes)
        print(f"[INFO] Found {total} distinct combinations of ({attr_names})")

    results = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(execute_query, val): val for val in distinct_values}
        with tqdm(total=total, desc="Executing queries") as pbar:
            for future in as_completed(futures):
                values = futures[future]
                try:
                    df = future.result()
                    if df is not None:  # Only append if df is not None
                        results.append(df)
                except Exception as e:
                    print(f"[ERROR] Query failed for values {values}: {e}")
                finally:
                    if verbose:
                        active = threading.active_count()
                        print(f"[PROGRESS] Completed values: {values} | Active workers: {active - 1}")
                    pbar.update(1)

    return pd.concat(results, ignore_index=True)