"""
Shared database connection utilities for the MaxAB pricing system.

Provides a single query_snowflake() implementation to replace the 12
duplicate definitions scattered across notebooks.

Usage in notebooks:
    import sys, os
    sys.path.insert(0, os.path.abspath('..'))  # if running from modules/
    from db import query_snowflake
"""

import os
import pandas as pd
import snowflake.connector

SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"


def query_snowflake(query: str, columns: list | None = None) -> pd.DataFrame:
    """
    Execute a SQL query against Snowflake and return results as a DataFrame.

    Connects using SNOWFLAKE_USERNAME, SNOWFLAKE_ACCOUNT, SNOWFLAKE_PASSWORD,
    and SNOWFLAKE_DATABASE environment variables (set by setup_environment_2).

    If ``columns`` is provided, use it as DataFrame column names; otherwise use
    Snowflake cursor descriptions (lowercased).
    """
    con = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USERNAME"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    try:
        cur = con.cursor()
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(query)
        data = cur.fetchall()
        if columns is not None:
            df = pd.DataFrame(data, columns=columns)
        else:
            col_names = [desc[0].lower() for desc in cur.description]
            df = pd.DataFrame(data, columns=col_names)
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
        return df
    finally:
        con.close()


def get_snowflake_timezone() -> str:
    """Return the Snowflake session TIMEZONE parameter."""
    df = query_snowflake("SHOW PARAMETERS LIKE 'TIMEZONE'")
    if len(df) > 0 and 'value' in df.columns:
        return df['value'].iloc[0]
    return "UTC"
