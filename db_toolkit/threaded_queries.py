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
    host, port, dbname, user, password,
    query_template: str,
    target_table,
    distinct_sources: dict,
    verbose: bool = True,
    debug: bool = False,
    max_combinations: int = None
):
    create_connection_pool(host, port, dbname, user, password)
    attributes = tuple(distinct_sources.keys())

    def fetch_distinct_values():
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

    def prepare_params(value_tuple, query_template):
        num_placeholders = query_template.count('%s')
        base_values = list(value_tuple)

        if len(base_values) < num_placeholders:
            repeats = (num_placeholders + len(base_values) - 1) // len(base_values)
            extended_values = (base_values * repeats)[:num_placeholders]
        elif len(base_values) > num_placeholders:
            raise ValueError(
                f"[ERROR] Too many input values ({len(base_values)}) for {num_placeholders} placeholders in query."
            )
        else:
            extended_values = base_values

        return tuple(extended_values)

    def execute_query(values, max_retries=3, base_delay=1):
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

                params = prepare_params(values, query_template)

                if debug:
                    print(f"[DEBUG] Attempt {attempt} - Query: {final_query.as_string(conn)}")

                with conn.cursor() as cur:
                    cur.execute(final_query, params)
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
                    if df is not None:
                        results.append(df)
                except Exception as e:
                    print(f"[ERROR] Query failed for values {values}: {e}")
                finally:
                    if verbose:
                        active = threading.active_count()
                        print(f"[PROGRESS] Completed values: {values} | Active workers: {active - 1}")
                    pbar.update(1)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
