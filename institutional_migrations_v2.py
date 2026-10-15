"""Extension of the restart-safe institutional migration ledger for schemas 026+.

The original migration runner remains immutable for the already-promoted 1-25 chain. This
module calls it first, then applies later migrations into the same `er_schema_migrations`
table with the same checksum-drift guarantees.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from db import connect
from institutional_migrations import ensure_schema as ensure_base_schema

EXTRA_MIGRATION_FILES = [
    "institutional_schema_026.sql",
    "institutional_schema_027.sql",
    "institutional_schema_028.sql",
    "institutional_schema_029.sql",
    "institutional_schema_030.sql",
    "institutional_schema_031.sql",
    "institutional_schema_032.sql",
    "institutional_schema_033.sql",
    "institutional_schema_034.sql",
    "institutional_schema_035.sql",
    "institutional_schema_036.sql",
    "institutional_schema_037.sql",
    "institutional_schema_038.sql",
    "institutional_schema_039.sql",
    "institutional_schema_040.sql",
    "institutional_schema_041.sql",
    "institutional_schema_042.sql",
    "institutional_schema_043.sql",
    "institutional_schema_044.sql",
    "institutional_schema_045.sql",
    "institutional_schema_046.sql",
    "institutional_schema_047.sql",
    "institutional_schema_048.sql",
    "institutional_schema_049.sql",
    "institutional_schema_050.sql",
    "institutional_schema_051.sql",
    "institutional_schema_052.sql",
    "institutional_schema_053.sql",
    "institutional_schema_054.sql",
]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_schema() -> dict:
    root = Path(__file__).parent
    base = ensure_base_schema()
    applied_now = list(base.get("applied_now", []))
    bootstrapped = list(base.get("bootstrapped", []))
    con = connect()
    try:
        for name in EXTRA_MIGRATION_FILES:
            sql = (root / name).read_text()
            checksum = _sha256(sql)
            row = con.execute(
                "SELECT content_sha256 FROM er_schema_migrations WHERE migration_name=?", [name]
            ).fetchone()
            if row is not None:
                if row[0] != checksum:
                    raise RuntimeError(
                        f"Institutional migration checksum drift for {name}: applied SQL differs from repository SQL. "
                        "Add a new migration instead of editing an applied migration."
                    )
                continue
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(sql)
                con.execute("""
                    INSERT INTO er_schema_migrations(migration_name,content_sha256,bootstrapped)
                    VALUES (?, ?, FALSE)
                """, [name, checksum])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            applied_now.append(name)
        total = int(con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0])
        return {
            "migration_count": total,
            "applied_now": applied_now,
            "bootstrapped": bootstrapped,
        }
    finally:
        con.close()
