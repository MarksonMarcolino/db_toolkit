"""
db_toolkit
==========

A lightweight and modular toolkit for secure and reusable database access
via SSH tunnels, threaded and synchronous SQL queries, and environment management.

Modules
-------
- ssh : Create secure SSH tunnels using `sshtunnel`.
- db_connection : Connect to PostgreSQL-compatible databases.
- threaded_queries : Execute SQL queries in parallel by attribute.
- sync_queries : Run single SQL queries synchronously.
- utils : Load and validate environment variables from `.env` files.
"""

from .ssh import create_ssh_tunnel
from .db_connection import (
    get_connection,
    release_connection,
    close_pool,
    create_connection_pool
)
from .threaded_queries import run_parallel_queries
from .sync_queries import run_query
from .utils import (
    load_env,
    get_env_variable,
    require_env,
    safe_identifier,
    log_query_retry,
    log_query_failure,
)

__all__ = [
    "create_ssh_tunnel",
    "create_connection_pool",
    "get_connection",
    "release_connection",
    "close_pool",
    "run_parallel_queries",
    "run_query",
    "load_env",
    "get_env_variable",
    "require_env",
    "safe_identifier",
    "log_query_retry",
    "log_query_failure",
]