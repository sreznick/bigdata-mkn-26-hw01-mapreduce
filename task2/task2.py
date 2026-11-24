# task2.py
import hashlib
import math
import struct

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

DEVICE, EVENT = 0, 1


class BloomFilter(object):

    def __init__(self, m, k, bits=None):
        self.m = m
        self.k = k
        self.bits = bits if bits is not None else bytearray((m + 7) // 8)

    @classmethod
    def create(cls, n, error_rate=0.01):
        m = math.ceil(-n * math.log(error_rate) / math.log(2) ** 2)
        k = round(m / n * math.log(2))
        return cls(max(m, 8), max(k, 1))

    def _hash_pair(self, key):
        digest = hashlib.md5(key.encode()).digest()
        return int.from_bytes(digest[:8], 'big'), int.from_bytes(digest[8:], 'big')

    def add(self, key):
        h1, h2 = self._hash_pair(key)
        for i in range(self.k):
            pos = (h1 + i * h2) % self.m
            self.bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, key):
        h1, h2 = self._hash_pair(key)
        for i in range(self.k):
            pos = (h1 + i * h2) % self.m
            if not self.bits[pos // 8] & (1 << (pos % 8)):
                return False
        return True

    def save(self, path):
        with open(path, 'wb') as f:
            f.write(struct.pack('<QQ', self.m, self.k))
            f.write(self.bits)

    @classmethod
    def load(cls, path):
        with open(path, 'rb') as f:
            m, k = struct.unpack('<QQ', f.read(16))
            return cls(m, k, bytearray(f.read()))


class EnrichEventsWithDeviceInfo(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol
    SORT_VALUES = True

    def configure_args(self):
        super(EnrichEventsWithDeviceInfo, self).configure_args()
        self.add_file_arg('--bloom-filter', dest='bloom_filter', default=None,
                          help='фильтр Блума по справочнику (build_bloom_filter.py)')

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper_init(self):
        path = self.options.bloom_filter
        self.bloom = BloomFilter.load(path) if path else None

    def mapper(self, _, line):
        parts = line.strip().split(',')
        if parts[0] in ('device_id', 'event_id'):
            return
        if len(parts) == 2:
            device_id, category = parts
            yield device_id, (DEVICE, category)
        elif len(parts) == 3:
            event_id, device_id, event_type = parts
            if not device_id:
                return
            if self.bloom is not None and device_id not in self.bloom:
                return
            yield device_id, (EVENT, event_id, event_type)

    def reducer(self, device_id, values):
        category = None
        for value in values:
            if value[0] == DEVICE:
                category = value[1]
            elif category is None:
                return
            else:
                yield None, f'{value[1]},{device_id},{value[2]},{category}'


if __name__ == '__main__':
    EnrichEventsWithDeviceInfo.run()