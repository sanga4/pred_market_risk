from pathlib import Path
from db import connect


def main():
    sql = Path(__file__).with_name("schema.sql").read_text()
    con = connect()
    con.execute(sql)
    con.close()
    print("schema ready")


if __name__ == "__main__":
    main()
