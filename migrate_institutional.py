from pathlib import Path
from db import connect


def main():
    sql = Path(__file__).with_name("institutional_schema.sql").read_text()
    con = connect()
    try:
        con.execute(sql)
        print("institutional schema ready")
    finally:
        con.close()


if __name__ == "__main__":
    main()
