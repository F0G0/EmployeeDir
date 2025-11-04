from __future__ import annotations

import sys
import time
from datetime import datetime, date
from typing import Iterable

from db import get_connection, create_table, create_performance_indexes, drop_all_indexes
from models.employee import Employee
from utils import generate_employees, generate_special_F_males, timer


def mode_1_create_table() -> None:
    conn = get_connection()
    create_table(conn)
    conn.close()
    print("Table 'employees' ensured.")


def mode_2_insert_one(args: list[str]) -> None:
    if len(args) < 3:
        print("Usage: 2 \"Last First Middle\" YYYY-MM-DD Gender")
        sys.exit(1)
    full_name = args[0]
    birth_date_str = args[1]
    gender = args[2]

    employee = Employee.from_strings(full_name, birth_date_str, gender)
    conn = get_connection()
    create_table(conn)
    employee.insert(conn)
    conn.close()
    print(f"Inserted: {employee.full_name} {employee.birth_date.isoformat()} {employee.gender}")


def mode_3_list_unique() -> None:
    conn = get_connection()
    create_table(conn)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT last_name, first_name, middle_name, birth_date,
                   MIN(gender) as gender
            FROM employees
            GROUP BY last_name, first_name, middle_name, birth_date
            ORDER BY last_name, first_name, middle_name
            """
        )
        rows = cur.fetchall()
        today = date.today()
        for ln, fn, mn, bd_str, gender in rows:
            bd = datetime.strptime(bd_str, "%Y-%m-%d").date()
            age = Employee(ln, fn, mn, bd, gender).age_years(today)
            print(f"{ln} {fn} {mn}\t{bd.isoformat()}\t{gender}\t{age}")
    finally:
        cur.close()
        conn.close()


def mode_4_generate_and_insert() -> None:
    conn = get_connection()
    create_table(conn)

    elapsed, _ = timer()

    total_main = 1000000
    total_special = 100

    gen_main = generate_employees(total_main)
    gen_special = generate_special_F_males(total_special)

    inserted_main = Employee.batch_insert(conn, gen_main, chunk_size=10000)
    print(f"Inserted main: {inserted_main}")

    inserted_special = Employee.batch_insert(conn, gen_special, chunk_size=500)
    print(f"Inserted F: {inserted_special}")

    total_time = elapsed()
    conn.close()
    print(f"Done. Total inserted: {inserted_main + inserted_special}. Time: {total_time:.3f}s")


def mode_5_timed_query() -> None:
    conn = get_connection()
    create_table(conn)
    cur = conn.cursor()
    try:
        start = time.perf_counter()
        cur.execute(
            """
            SELECT last_name, first_name, middle_name, birth_date, gender
            FROM employees
            WHERE gender = 'Male' AND last_name LIKE 'F%'
            ORDER BY last_name, first_name, middle_name
            """
        )
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
        print(f"Rows: {len(rows)}. Time: {elapsed:.6f}s")
    finally:
        cur.close()
        conn.close()


def mode_6_optimize_and_compare() -> None:
    conn = get_connection()
    create_table(conn)
    cur = conn.cursor()
    try:
        drop_all_indexes(conn)
        start = time.perf_counter()
        cur.execute(
            """
            SELECT last_name, first_name, middle_name, birth_date, gender
            FROM employees
            WHERE gender = 'Male' AND last_name LIKE 'F%'
            ORDER BY last_name, first_name, middle_name
            """
        )
        rows = cur.fetchall()
        baseline = time.perf_counter() - start

        create_performance_indexes(conn)
        start2 = time.perf_counter()
        cur.execute(
            """
            SELECT last_name, first_name, middle_name, birth_date, gender
            FROM employees
            WHERE gender = 'Male' AND last_name LIKE 'F%'
            ORDER BY last_name, first_name, middle_name
            """
        )
        rows2 = cur.fetchall()
        optimized = time.perf_counter() - start2

        print(
            f"Baseline rows: {len(rows)} time: {baseline:.6f}s | "
            f"Optimized rows: {len(rows2)} time: {optimized:.6f}s"
        )

    finally:
        cur.close()
        conn.close()


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(1)

    mode = sys.argv[1]
    match mode:
        case "1":
            mode_1_create_table()
        case "2":
            mode_2_insert_one(sys.argv[2:])
        case "3":
            mode_3_list_unique()
        case "4":
            mode_4_generate_and_insert()
        case "5":
            mode_5_timed_query()
        case "6":
            mode_6_optimize_and_compare()
        case _:
            print(f"Unknown parameter: {mode}")
            sys.exit(1)

if __name__ == "__main__":
    main()


