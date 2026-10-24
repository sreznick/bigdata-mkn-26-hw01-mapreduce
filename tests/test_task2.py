import os
import random
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "task2"))

from bloom import BloomFilter, ShardedBloomFilter  # noqa: E402
from build_bloom import BuildDeviceBloomFilter  # noqa: E402
from task2 import EnrichEventsWithDeviceInfo  # noqa: E402

A_HEADER = "device_id,trust_category"
B_HEADER = "event_id,device_id,event_type"


def run(job_class, args, runner="inline"):
    job = job_class(["-r", runner, "--no-conf"] + [str(a) for a in args])
    with job.make_runner() as r:
        r.run()
        return sorted(line.rstrip("\n")
                      for _, line in job.parse_output(r.cat_output()))


def build_bloom(tmp_path, a_paths, capacity, shards=4, runner="inline"):
    lines = run(BuildDeviceBloomFilter,
                ["--capacity", capacity, "--shards", shards] + a_paths, runner)
    path = tmp_path / "bloom.txt"
    path.write_text("".join(line + "\n" for line in lines))
    return path


def brute_force(a_paths, b_paths):
    devices = {}
    for path in a_paths:
        for line in open(path):
            line = line.strip()
            if line and line != A_HEADER:
                device_id, trust = line.split(",")
                devices[device_id] = trust
    result = []
    for path in b_paths:
        for line in open(path):
            line = line.strip()
            if not line or line == B_HEADER:
                continue
            event_id, device_id, event_type = line.split(",")
            if device_id in devices:
                result.append("%s,%s,%s,%s" % (event_id, device_id, event_type,
                                               devices[device_id]))
    return sorted(result)


def write(path, header, lines):
    path.write_text("".join(l + "\n" for l in [header] + lines))
    return path


def test_hand_case(tmp_path):
    a = write(tmp_path / "a.csv", A_HEADER,
              ["aa:01,trusted", "aa:02,blocked", "aa:03,suspicious"])
    b = write(tmp_path / "b.csv", B_HEADER, [
        "e1,aa:01,view",
        "e2,,click",            # empty device_id
        "e3,zz:99,view",        # unknown device
        "e4,aa:02,purchase",
        "e5,aa:01,click",       # second event of the same device
        "e6,device_id,view",    # must not match the header of A
    ])
    expected = [
        "e1,aa:01,view,trusted",
        "e4,aa:02,purchase,blocked",
        "e5,aa:01,click,trusted",
    ]
    assert run(EnrichEventsWithDeviceInfo, [a, b]) == expected
    # order of input files does not matter
    assert run(EnrichEventsWithDeviceInfo, [b, a]) == expected
    bloom = build_bloom(tmp_path, [a], capacity=10)
    assert run(EnrichEventsWithDeviceInfo, ["--bloom", bloom, a, b]) == expected


def test_no_known_devices(tmp_path):
    a = write(tmp_path / "a.csv", A_HEADER, ["aa:01,trusted"])
    b = write(tmp_path / "b.csv", B_HEADER, ["e1,,view", "e2,bb:01,view"])
    assert run(EnrichEventsWithDeviceInfo, [a, b]) == []


def mac(rnd):
    return ":".join("%02x" % rnd.randrange(256) for _ in range(6))


def generate(tmp_path, seed, n_devices, n_events, n_files):
    rnd = random.Random(seed)
    devices = list({mac(rnd) for _ in range(n_devices)})
    categories = ["trusted", "suspicious", "blocked"]
    a_lines = [[] for _ in range(n_files)]
    for device_id in devices:
        rnd.choice(a_lines).append("%s,%s" % (device_id, rnd.choice(categories)))
    # hot devices get many events
    hot = devices[:3]
    b_lines = [[] for _ in range(n_files)]
    for i in range(n_events):
        p = rnd.random()
        if p < 0.03:
            device_id = rnd.choice(devices)
        elif p < 0.05:
            device_id = rnd.choice(hot)
        elif p < 0.5:
            device_id = ""
        else:
            device_id = mac(rnd)
        event_type = rnd.choice(["view", "click", "purchase"])
        rnd.choice(b_lines).append("ev%d,%s,%s" % (i, device_id, event_type))
    a_paths = [write(tmp_path / ("a%d.csv" % k), A_HEADER, lines)
               for k, lines in enumerate(a_lines)]
    b_paths = [write(tmp_path / ("b%d.csv" % k), B_HEADER, lines)
               for k, lines in enumerate(b_lines)]
    return a_paths, b_paths, len(devices)


@pytest.mark.parametrize("seed", range(10))
def test_random_inline(tmp_path, seed):
    a_paths, b_paths, n = generate(tmp_path, seed, 200, 3000, n_files=3)
    expected = brute_force(a_paths, b_paths)
    assert run(EnrichEventsWithDeviceInfo, a_paths + b_paths) == expected
    bloom = build_bloom(tmp_path, a_paths, capacity=n)
    assert run(EnrichEventsWithDeviceInfo,
               ["--bloom", bloom] + a_paths + b_paths) == expected


def test_random_local_runner(tmp_path):
    a_paths, b_paths, n = generate(tmp_path, 100, 2000, 30000, n_files=4)
    expected = brute_force(a_paths, b_paths)
    bloom = build_bloom(tmp_path, a_paths, capacity=n, runner="local")
    assert run(EnrichEventsWithDeviceInfo,
               ["--bloom", bloom] + a_paths + b_paths,
               runner="local") == expected


def test_bloom_false_positive_rate(tmp_path):
    rnd = random.Random(1)
    known = list({mac(rnd) for _ in range(5000)})
    a = write(tmp_path / "a.csv", A_HEADER, ["%s,trusted" % d for d in known])
    bloom = ShardedBloomFilter.load(
        build_bloom(tmp_path, [a], capacity=len(known), shards=8))

    assert all(d in bloom for d in known)
    known_set = set(known)
    unknown = [d for d in (mac(rnd) for _ in range(20000)) if d not in known_set]
    false_positives = sum(d in bloom for d in unknown)
    # target error rate is 1%
    assert false_positives / len(unknown) < 0.02


def test_bloom_filter_roundtrip():
    bloom = BloomFilter.for_capacity(100, 0.01)
    bloom.add_hashes(12345, 67891)
    decoded = BloomFilter.decode(bloom.encode())
    assert decoded.num_bits == bloom.num_bits
    assert decoded.num_hashes == bloom.num_hashes
    assert decoded.contains_hashes(12345, 67891)
