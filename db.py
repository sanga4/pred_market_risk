import os
import duckdb

DB = os.getenv("DB", "md:event_risk")


def connect(read_only=False):
    # MotherDuck manages concurrent remote readers/writers.  For local staging we run the
    # API and collector in one process, so all connections must use the same DuckDB config.
    if os.getenv("DB_FORCE_READ_WRITE", "false").lower() in {"1", "true", "yes"}:
        read_only = False
    if DB.startswith("md:"):
        name = DB[3:]
        con = duckdb.connect("md:")
        con.execute(f'USE "{name}"')
        return con
    return duckdb.connect(DB, read_only=read_only)
