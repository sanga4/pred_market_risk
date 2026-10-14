from pathlib import Path

from db import connect


MIGRATIONS = [
    "institutional_schema.sql",
    "institutional_schema_002.sql",
    "institutional_schema_003.sql",
    "institutional_schema_004.sql",
    "institutional_schema_005.sql",
    "institutional_schema_006.sql",
]


def main():
    root = Path(__file__).parent
    con = connect()
    try:
        for name in MIGRATIONS:
            sql = (root / name).read_text()
            con.execute(sql)
            print(f"applied {name}")
        print("institutional schema ready")
    finally:
        con.close()


if __name__ == "__main__":
    main()
