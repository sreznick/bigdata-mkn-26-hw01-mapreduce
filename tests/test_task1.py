import os
import random
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "task1"))

from task1 import Top3WithBorder  # noqa: E402

HEADER = "row_id," + ",".join("metric_%d" % i for i in range(1, 10))


def run_job(paths, runner="inline"):
    job = Top3WithBorder(["-r", runner, "--no-conf"] + [str(p) for p in paths])
    with job.make_runner() as r:
        r.run()
        return sorted(line.rstrip("\n")
                      for _, line in job.parse_output(r.cat_output()))


def brute_force(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            for line in f:
                parts = line.strip().split(",")
                if parts[0] == "row_id":
                    continue
                rows.append((parts[0], [float(x) for x in parts[1:]]))

    result = []
    for m in range(9):
        col = sorted(((r[1][m], r[0]) for r in rows if r[1][m] == r[1][m]),
                     key=lambda t: (-t[0], t[1]))
        values = [v for v, _ in col]
        third = values[2] if len(values) > 2 else None
        fourth = values[3] if len(values) > 3 else None
        top = [i for v, i in col if fourth is None or v > fourth]
        border = 0
        if fourth is not None and fourth == third:
            border = values.count(third)
        result.append("%d,%s,%d" % (m + 1, ";".join(top), border))
    return sorted(result)


def write_column(tmp_path, rows, header=False):
    """Writes rows (row_id, value) with the same value in all 9 metrics."""
    path = tmp_path / "input.csv"
    lines = [HEADER] if header else []
    lines += ["%s,%s" % (row_id, ",".join([value] * 9)) for row_id, value in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def expected_all(line):
    return sorted("%d,%s" % (m, line) for m in range(1, 10))


@pytest.mark.parametrize("rows, expected", [
    # example from README
    ([("a", "10"), ("b", "200"), ("c", "150"), ("d", "150"), ("e", "150")],
     "b,3"),
    # all three places are unambiguous
    ([("a", "1"), ("b", "4"), ("c", "3"), ("d", "2")], "b;c;d,0"),
    # tie inside the top that is still unambiguous
    ([("a", "200"), ("b", "200"), ("c", "150"), ("d", "10")], "a;b;c,0"),
    # tie on the 3rd and 4th places
    ([("a", "300"), ("b", "200"), ("c", "150"), ("d", "150"), ("e", "150")],
     "a;b,3"),
    # tie covering the whole top
    ([("a", "5"), ("b", "5"), ("c", "5"), ("d", "5"), ("e", "1")], ",4"),
    # border starts at the 2nd place
    ([("a", "9"), ("b", "5"), ("c", "5"), ("d", "5")], "a,3"),
    # fewer than 4 rows: no 4th place, everything is unambiguous
    ([("a", "1"), ("b", "2")], "b;a,0"),
    ([("a", "7"), ("b", "7"), ("c", "7")], "a;b;c,0"),
    # equal numbers written differently, negatives
    ([("a", "1"), ("b", "1.0"), ("c", "-3"), ("d", "1e0"), ("e", "-2.5"),
      ("f", "1.00")], ",4"),
    ([("a", "1"), ("b", "1.0"), ("c", "-3"), ("d", "1e0"), ("e", "-2.5")],
     "a;b;d,0"),
    # NaN does not take part in the ranking
    ([("a", "nan"), ("b", "1"), ("c", "2"), ("d", "3"), ("e", "0")],
     "d;c;b,0"),
    # infinities
    ([("a", "inf"), ("b", "-inf"), ("c", "0"), ("d", "-inf"), ("e", "-inf")],
     "a;c,3"),
])
def test_hand_cases(tmp_path, rows, expected):
    path = write_column(tmp_path, rows)
    assert run_job([path]) == expected_all(expected)
    assert brute_force([path]) == expected_all(expected)


def test_header_is_skipped(tmp_path):
    rows = [("a", "10"), ("b", "200"), ("c", "150"), ("d", "150"), ("e", "150")]
    path = write_column(tmp_path, rows, header=True)
    assert run_job([path]) == expected_all("b,3")


def test_metrics_are_independent(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("\n".join(
        "r%d,%s" % (i, ",".join(str((i * (m + 1)) % 7) for m in range(9)))
        for i in range(20)) + "\n")
    assert run_job([path]) == brute_force([path])


def generate(tmp_path, seed, n_rows, n_files):
    """Random input split into several files, with many duplicate values."""
    rnd = random.Random(seed)
    ranges = [rnd.choice([1, 2, 3, 5, 10, 1000, 10 ** 6]) for _ in range(9)]
    paths = [tmp_path / ("part%d.csv" % k) for k in range(n_files)]
    lines = [[] for _ in paths]
    lines[0].append(HEADER)
    for r in range(n_rows):
        values = []
        for m, k in enumerate(ranges):
            v = rnd.randint(0, k)
            if m % 3 == 0:
                values.append(rnd.choice([str(v), "%d.0" % v, str(-v)]))
            else:
                values.append(repr(v * 1.5))
        rnd.choice(lines).append("r%d,%s" % (r, ",".join(values)))
    for path, file_lines in zip(paths, lines):
        path.write_text("".join(line + "\n" for line in file_lines))
    return paths


@pytest.mark.parametrize("seed", range(40))
def test_random_inline(tmp_path, seed):
    paths = generate(tmp_path, seed, n_rows=seed * 3 + seed % 8, n_files=3)
    assert run_job(paths) == brute_force(paths)


@pytest.mark.parametrize("seed", [100, 101])
def test_random_local_runner(tmp_path, seed):
    # separate processes, several mappers and the combiner
    paths = generate(tmp_path, seed, n_rows=20000, n_files=4)
    assert run_job(paths, runner="local") == brute_force(paths)
