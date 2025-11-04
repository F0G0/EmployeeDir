from __future__ import annotations

import random
import string
import time
from datetime import date, timedelta
from typing import Generator, Iterable

from models.employee import Employee


def timer() -> tuple[callable, callable]:
    start_time = time.perf_counter()

    def elapsed() -> float:
        return time.perf_counter() - start_time

    def restart() -> None:
        nonlocal start_time
        start_time = time.perf_counter()

    return elapsed, restart


FIRST_NAMES_MALE = [
    "Ivan", "Petr", "Sergey", "Alexey", "Dmitry", "Fedor", "Nikolay", "Andrey",
]
FIRST_NAMES_FEMALE = [
    "Anna", "Elena", "Maria", "Olga", "Natalia", "Irina", "Sofia", "Tatiana",
]
MIDDLE_NAMES = [
    "Ivanovich", "Petrovich", "Sergeevich", "Alexeevich", "Dmitrievich", "Nikolaevich",
    "Ivanovna", "Petrovna", "Sergeevna", "Alexeevna", "Dmitrievna", "Nikolaevna",
]
LAST_NAME_PREFIXES = list(string.ascii_uppercase)


def random_birth_date(rng: random.Random) -> date:
    today = date.today()
    start = today - timedelta(days=70 * 365)
    end = today - timedelta(days=18 * 365)
    days_range = (end - start).days
    return start + timedelta(days=rng.randint(0, days_range))


def generate_employees(total: int, seed: int = 123) -> Generator[Employee, None, None]:
    rng = random.Random(seed)
    for i in range(total):
        gender = "Male" if i % 2 == 0 else "Female"
        if gender == "Male":
            first = rng.choice(FIRST_NAMES_MALE)
            middle = rng.choice([m for m in MIDDLE_NAMES if m.endswith("ich")])
        else:
            first = rng.choice(FIRST_NAMES_FEMALE)
            middle = rng.choice([m for m in MIDDLE_NAMES if m.endswith("na")])
        last_initial = LAST_NAME_PREFIXES[i % len(LAST_NAME_PREFIXES)]
        last = f"{last_initial}{rng.choice(['edorov','ilin','aev','ov','in','sky','son'])}"
        yield Employee(
            last_name=last,
            first_name=first,
            middle_name=middle,
            birth_date=random_birth_date(rng),
            gender=gender,
        )


def generate_special_F_males(count: int, seed: int = 123) -> Generator[Employee, None, None]:
    rng = random.Random(seed)
    for _ in range(count):
        last = f"F{rng.choice(['edorov','ilin','aev','ov','in','sky','son'])}"
        first = rng.choice(FIRST_NAMES_MALE)
        middle = rng.choice([m for m in MIDDLE_NAMES if m.endswith("ich")])
        yield Employee(
            last_name=last,
            first_name=first,
            middle_name=middle,
            birth_date=random_birth_date(rng),
            gender="Male",
        )


