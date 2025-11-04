import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional

DB_FILE = "employees.db"


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DB_FILE
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Cursor]:
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def create_table(conn: sqlite3.Connection) -> None:
    with transaction(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_name TEXT NOT NULL,
                first_name TEXT NOT NULL,
                middle_name TEXT NOT NULL,
                birth_date TEXT NOT NULL, -- ISO format YYYY-MM-DD
                gender TEXT NOT NULL CHECK (gender IN ('Male','Female'))
            );
            """
        )


def create_performance_indexes(conn: sqlite3.Connection) -> None:
    with transaction(conn) as cur:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_employees_gender_lastname ON employees(gender, last_name);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_employees_lastname_birthdate ON employees(last_name, birth_date);"
        )


def drop_all_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_employees_%';"
        )
        index_names = [row[0] for row in cur.fetchall()]
        for name in index_names:
            cur.execute(f"DROP INDEX IF EXISTS {name};")
        conn.commit()
    finally:
        cur.close()

    old_isolation = conn.isolation_level
    try:
        conn.isolation_level = None
        conn.execute("VACUUM;")
        conn.execute("ANALYZE;")
    finally:
        conn.isolation_level = old_isolation


