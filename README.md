# db_toolkit

A lightweight and modular Python toolkit for secure, reusable access to cloud-hosted databases via SSH, with support for parallel and synchronous SQL queries.

## Features

- Secure SSH tunneling with `sshtunnel`
- PostgreSQL-compatible DB connections with `psycopg2`
- Threaded SQL execution using `ThreadPoolExecutor`
- Synchronous query support
- Returns results as `pandas.DataFrame`
- Environment variable management with `python-dotenv`

## Installation

### Option 1: From GitHub
```bash
pip install git+ssh://git@github.com/MarksonMarcolino/db_toolkit.git
```

### Option 2: From Local Source (for development)
```bash
git clone git@github.com:MarksonMarcolino/db_toolkit.git
cd db_toolkit
pip install -e .
```

## Project Structure

```bash
.
├── db_toolkit/              # Source code
│   ├── __init__.py
│   ├── db_connection.py
│   ├── ssh.py
│   ├── sync_queries.py
│   ├── threaded_queries.py
│   └── utils.py
├── example_notebook.ipynb   # Generic usage example
├── .env.sample              # Sample environment file
├── requirements.txt
├── README.md
├── docs/                    # Sphinx documentation
│   ├── conf.py
│   ├── index.rst
│   └── ...
└── setup.py
```

## Quickstart

```python
from db_toolkit import (
    load_env, get_env_variable, require_env,
    create_ssh_tunnel, run_parallel_queries, run_query
)

load_env()
require_env([
    "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
    "SSH_HOST", "SSH_USER", "SSH_PRIVATE_KEY"
])

# Start SSH tunnel
ssh_tunnel = create_ssh_tunnel(
    ssh_host=get_env_variable("SSH_HOST"),
    ssh_port=22,
    ssh_username=get_env_variable("SSH_USER"),
    ssh_private_key=get_env_variable("SSH_PRIVATE_KEY"),
    remote_bind_address=(get_env_variable("DB_HOST"), int(get_env_variable("DB_PORT")))
)

# Example: Run parallel queries
query_template = """
SELECT 
    *
FROM {table}
WHERE {attribute_0} = %s AND {attribute_1} = %s
"""

df = run_parallel_queries(
    host="localhost",
    port=ssh_tunnel.local_bind_port,
    dbname=get_env_variable("DB_NAME"),
    user=get_env_variable("DB_USER"),
    password=get_env_variable("DB_PASSWORD"),
    query_template=query_template,
    target_table=("your_schema", "your_table"),
    distinct_sources={
        "attribute_0": ("your_schema", "your_table"),
        "attribute_1": ("your_schema", "your_other_table")
    },
    verbose=True,
    debug=False
)

ssh_tunnel.stop()
```

---

## Tags

[![pandas](https://img.shields.io/pypi/v/pandas.svg?label=pandas&color=blue)](https://pypi.org/project/pandas/)
[![psycopg2-binary](https://img.shields.io/pypi/v/psycopg2-binary.svg?label=psycopg2-binary&color=blue)](https://pypi.org/project/psycopg2-binary/)
[![sshtunnel](https://img.shields.io/pypi/v/sshtunnel.svg?label=sshtunnel&color=blue)](https://pypi.org/project/sshtunnel/)
[![python-dotenv](https://img.shields.io/pypi/v/python-dotenv.svg?label=python-dotenv&color=blue)](https://pypi.org/project/python-dotenv/)
[![tqdm](https://img.shields.io/pypi/v/tqdm.svg?label=tqdm&color=blue)](https://pypi.org/project/tqdm/)
[![setuptools](https://img.shields.io/pypi/v/setuptools.svg?label=setuptools&color=blue)](https://pypi.org/project/setuptools/)

---

## License

MIT License

---

## Author

Markson Rebelo Marcolino
