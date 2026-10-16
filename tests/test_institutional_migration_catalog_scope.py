import duckdb

from institutional_migrations import _column_exists, _table_exists, _view_exists


def test_migration_discovery_ignores_objects_in_other_catalogs():
    con = duckdb.connect()
    try:
        con.execute("ATTACH ':memory:' AS production")
        con.execute("CREATE TABLE production.main.only_there(id INTEGER, marker VARCHAR)")
        con.execute("CREATE VIEW production.main.only_view AS SELECT 1 AS value")

        assert not _table_exists(con, "only_there")
        assert not _column_exists(con, "only_there", "marker")
        assert not _view_exists(con, "only_view")

        con.execute("CREATE TABLE main.only_there(id INTEGER, marker VARCHAR)")
        con.execute("CREATE VIEW main.only_view AS SELECT 1 AS value")

        assert _table_exists(con, "only_there")
        assert _column_exists(con, "only_there", "marker")
        assert _view_exists(con, "only_view")
    finally:
        con.close()
