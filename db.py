import os
import duckdb

DB = os.getenv("DB", "md:geo_odds")


def connect(read_only=False):
    if DB.startswith("md:"):
        name = DB[3:]
        con = duckdb.connect("md:")
        con.execute(f'USE "{name}"')
        return con
    return duckdb.connect(DB, read_only=read_only)
