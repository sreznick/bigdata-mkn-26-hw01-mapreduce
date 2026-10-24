import os
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "task3"))

from task3 import SuspiciousTransactions  # noqa: E402

HEADER = "transaction_id,user_id,amount,timestamp"


def run_job(paths, runner="inline"):
    job = SuspiciousTransactions(["-r", runner, "--no-conf"] + [str(p) for p in paths])
    with job.make_runner() as r:
        r.run()
        return [line.rstrip("\n") for _, line in job.parse_output(r.cat_output())]


def canonical(lines):
    """Pair order inside a line does not matter; total is compared as a number."""
    result = []
    for line in lines:
        user_id, t1, t2, total = line.split(",")
        result.append((user_id, min(t1, t2), max(t1, t2), Decimal(total)))
    return sorted(result)


def brute_force(paths):
    rows = []
    for path in paths:
        for line in open(path):
            line = line.strip()
            if line and line != HEADER:
                tid, uid, amount, ts = line.split(",")
                rows.append((tid, uid, Decimal(amount), date.fromisoformat(ts[:10])))
    result = []
    for i, (t1, u1, a1, d1) in enumerate(rows):
        for t2, u2, a2, d2 in rows[i + 1:]:
            if u1 == u2 and abs((d1 - d2).days) == 1 and a1 + a2 > Decimal(10000):
                result.append("%s,%s,%s,%s" % (u1, t1, t2, a1 + a2))
    return canonical(result)


def write(tmp_path, lines, name="input.csv", header=True):
    path = tmp_path / name
    path.write_text("".join(l + "\n" for l in ([HEADER] if header else []) + lines))
    return path


def test_readme_example(tmp_path):
    path = write(tmp_path, [
        "t1,u1,6000,2023-10-01T10:00:00",
        "t2,u1,5000,2023-10-02T09:00:00",
        "t3,u1,7000,2023-10-02T14:00:00",
        "t4,u1,3000,2023-10-03T11:00:00",
    ])
    assert sorted(run_job([path])) == ["u1,t1,t2,11000", "u1,t1,t3,13000"]


def test_earlier_transaction_goes_first(tmp_path):
    path = write(tmp_path, [
        "late,u1,6000,2023-10-02T10:00:00",
        "early,u1,6000,2023-10-01T10:00:00",
    ])
    assert run_job([path]) == ["u1,early,late,12000"]


@pytest.mark.parametrize("ts_1, ts_2, adjacent", [
    # calendar days, not 24 hours
    ("2023-10-01T23:59:59", "2023-10-02T00:00:00", True),
    ("2023-10-01T00:00:00", "2023-10-02T23:59:59", True),
    ("2023-10-01T00:00:00", "2023-10-01T23:59:59", False),
    ("2023-10-01T23:59:59", "2023-10-03T00:00:00", False),
    # month, year and leap day boundaries
    ("2023-09-30T12:00:00", "2023-10-01T12:00:00", True),
    ("2023-12-31T12:00:00", "2024-01-01T12:00:00", True),
    ("2024-02-28T12:00:00", "2024-02-29T12:00:00", True),
    ("2024-02-29T12:00:00", "2024-03-01T12:00:00", True),
    ("2024-02-28T12:00:00", "2024-03-01T12:00:00", False),
    ("2023-02-28T12:00:00", "2023-03-01T12:00:00", True),
])
def test_adjacent_days(tmp_path, ts_1, ts_2, adjacent):
    path = write(tmp_path, ["a,u1,6000,%s" % ts_1, "b,u1,6000,%s" % ts_2])
    assert run_job([path]) == (["u1,a,b,12000"] if adjacent else [])


@pytest.mark.parametrize("amount_1, amount_2, expected", [
    ("5000", "5000", None),                            # equal to threshold
    ("5000", "5000.01", "10000.01"),
    # float would give exactly 10000.0 here
    ("5000", "5000.0000000000001", "10000.0000000000001"),
    ("0.5", "9999.5", None),
    ("10000.5", "0.25", "10000.75"),
    ("6000.50", "5000.25", "11000.75"),
])
def test_exact_amounts(tmp_path, amount_1, amount_2, expected):
    path = write(tmp_path, [
        "a,u1,%s,2023-10-01T10:00:00" % amount_1,
        "b,u1,%s,2023-10-02T10:00:00" % amount_2,
    ])
    result = run_job([path])
    if expected is None:
        assert result == []
    else:
        assert len(result) == 1 and Decimal(result[0].split(",")[3]) == Decimal(expected)


def test_users_are_not_mixed(tmp_path):
    path = write(tmp_path, [
        "a,u1,6000,2023-10-01T10:00:00",
        "b,u2,6000,2023-10-02T10:00:00",
        "c,u2,6000,2023-10-03T10:00:00",
    ])
    assert run_job([path]) == ["u2,b,c,12000"]


def test_many_transactions_per_day(tmp_path):
    lines = ["d1_%d,u1,%d,2023-10-01T%02d:00:00" % (i, 1000 * i, i) for i in range(1, 11)]
    lines += ["d2_%d,u1,%d,2023-10-02T%02d:00:00" % (i, 1000 * i, i) for i in range(1, 11)]
    path = write(tmp_path, lines)
    result = run_job([path])
    assert canonical(result) == brute_force([path])
    # 1000*i + 1000*j > 10000  <=>  i + j > 10
    assert len(result) == sum(1 for i in range(1, 11) for j in range(1, 11) if i + j > 10)


def generate(tmp_path, seed, n_users, n_rows, n_files):
    rnd = random.Random(seed)
    start = datetime(2023, 12, 25)
    lines = [[] for _ in range(n_files)]
    for i in range(n_rows):
        user_id = "u%d" % rnd.randrange(n_users)
        ts = start + timedelta(seconds=rnd.randrange(14 * 24 * 3600))
        cents = rnd.choice([rnd.randint(1, 1000000), rnd.randint(400000, 600000), 500000])
        amount = "%d.%02d" % divmod(cents, 100)
        if rnd.random() < 0.3:
            amount = str(cents // 100)
        rnd.choice(lines).append("t%d,%s,%s,%s" % (i, user_id, amount, ts.isoformat()))
    return [write(tmp_path, file_lines, name="part%d.csv" % k, header=(k == 0))
            for k, file_lines in enumerate(lines)]


@pytest.mark.parametrize("seed", range(15))
def test_random_inline(tmp_path, seed):
    paths = generate(tmp_path, seed, n_users=5 + seed, n_rows=300, n_files=3)
    result = run_job(paths)
    assert canonical(result) == brute_force(paths)
    assert len(result) == len(set(canonical(result)))


def test_random_local_runner(tmp_path):
    paths = generate(tmp_path, 100, n_users=200, n_rows=8000, n_files=4)
    assert canonical(run_job(paths, runner="local")) == brute_force(paths)
