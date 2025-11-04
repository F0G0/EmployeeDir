from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, List, Sequence


@dataclass
class Employee:
    last_name: str
    first_name: str
    middle_name: str
    birth_date: date
    gender: str

    @property
    def full_name(self) -> str:
        return f"{self.last_name} {self.first_name} {self.middle_name}"

    def age_years(self, on_date: date | None = None) -> int:
        today = on_date or date.today()
        years = today.year - self.birth_date.year
        if (today.month, today.day) < (self.birth_date.month, self.birth_date.day):
            years -= 1
        return years

    @staticmethod
    def parse_full_name(full_name: str) -> Sequence[str]:
        parts = full_name.strip().split()
        if len(parts) != 3:
            raise ValueError("Change name to 'Last First Middle'")
        return parts[0], parts[1], parts[2]

    @classmethod
    def from_strings(cls, full_name: str, birth_date_str: str, gender: str) -> Employee:
        last_name, first_name, middle_name = cls.parse_full_name(full_name)
        try:
            bd = datetime.strptime(birth_date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Change to YYYY-MM-DD format") from exc
        gender_norm = gender.strip().capitalize()
        if gender_norm not in ("Male", "Female"):
            raise ValueError("Change to 'Male' or 'Female'")
        return cls(
            last_name=last_name,
            first_name=first_name,
            middle_name=middle_name,
            birth_date=bd,
            gender=gender_norm,
        )

    def insert(self, conn) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO employees (last_name, first_name, middle_name, birth_date, gender)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self.last_name,
                    self.first_name,
                    self.middle_name,
                    self.birth_date.isoformat(),
                    self.gender,
                ),
            )
            conn.commit()
        finally:
            cur.close()

    @staticmethod
    def batch_insert(conn, employees: Iterable[Employee], chunk_size: int = 10000) -> int:
        cur = conn.cursor()
        total = 0
        try:
            batch: List[Employee] = []
            for emp in employees:
                batch.append(emp)
                if len(batch) >= chunk_size:
                    cur.executemany(
                        """
                        INSERT INTO employees (last_name, first_name, middle_name, birth_date, gender)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                e.last_name,
                                e.first_name,
                                e.middle_name,
                                e.birth_date.isoformat(),
                                e.gender,
                            )
                            for e in batch
                        ],
                    )
                    conn.commit()
                    total += len(batch)
                    batch.clear()
            if batch:
                cur.executemany(
                    """
                    INSERT INTO employees (last_name, first_name, middle_name, birth_date, gender)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            e.last_name,
                            e.first_name,
                            e.middle_name,
                            e.birth_date.isoformat(),
                            e.gender,
                        )
                        for e in batch
                    ],
                )
                conn.commit()
                total += len(batch)
            return total
        finally:
            cur.close()


