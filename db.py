import os
import duckdb

DB = os.getenv("DB", "md:event_risk")


def connect(read_only=False):
    """Open an EventRisk database session with deterministic UTC timestamp semantics.

    MotherDuck can expose a source/session timezone label such as `Etc/Unknown`. Python
    clients (notably Arrow/pytz conversion) cannot safely materialize that label. EventRisk
    stores and compares institutional timestamps in UTC, so every connection explicitly sets
    the session timezone before returning it.
    """
    if os.getenv("DB_FORCE_READ_WRITE", "false").lower() in {"1", "true", "yes"}:
        read_only = False
    if DB.startswith("md:"):
        name = DB[3:]
        con = duckdb.connect("md:")
        con.execute(f'USE "{name}"')
        con.execute("SET TimeZone='UTC'")
        return con
    con = duckdb.connect(DB, read_only=read_only)
    con.execute("SET TimeZone='UTC'")
    return con
