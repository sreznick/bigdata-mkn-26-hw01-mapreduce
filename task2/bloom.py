# bloom.py
import base64
import hashlib
import math
import struct

HEADER = struct.Struct("<QB")


def key_hashes(key):
    digest = hashlib.blake2b(key.encode(), digest_size=24).digest()
    h1, h2, h3 = struct.unpack("<QQQ", digest)
    return h1, h2 | 1, h3


class BloomFilter:

    def __init__(self, num_bits, num_hashes):
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self.bits = bytearray((num_bits + 7) // 8)

    @classmethod
    def for_capacity(cls, capacity, error_rate):
        capacity = max(capacity, 1)
        num_bits = math.ceil(-capacity * math.log(error_rate) / math.log(2) ** 2)
        num_hashes = max(1, round(num_bits / capacity * math.log(2)))
        return cls(num_bits, num_hashes)

    def _positions(self, h1, h2):
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.num_bits

    def add_hashes(self, h1, h2):
        for pos in self._positions(h1, h2):
            self.bits[pos >> 3] |= 1 << (pos & 7)

    def contains_hashes(self, h1, h2):
        return all(self.bits[pos >> 3] & (1 << (pos & 7))
                   for pos in self._positions(h1, h2))

    def encode(self):
        header = HEADER.pack(self.num_bits, self.num_hashes)
        return base64.b64encode(header + bytes(self.bits)).decode()

    @classmethod
    def decode(cls, text):
        data = base64.b64decode(text)
        num_bits, num_hashes = HEADER.unpack_from(data)
        bloom = cls(num_bits, num_hashes)
        bloom.bits[:] = data[HEADER.size:]
        return bloom


class ShardedBloomFilter:
    """Bloom filter split into independent shards so that each shard can be
    built by its own reducer. A key goes to shard h3 % num_shards."""

    def __init__(self, num_shards):
        self.num_shards = num_shards
        self.shards = {}

    def __contains__(self, key):
        h1, h2, h3 = key_hashes(key)
        shard = self.shards.get(h3 % self.num_shards)
        return shard is not None and shard.contains_hashes(h1, h2)

    @classmethod
    def load(cls, path):
        bloom = None
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                shard, num_shards, encoded = line.split(",")
                if bloom is None:
                    bloom = cls(int(num_shards))
                elif bloom.num_shards != int(num_shards):
                    raise ValueError("Inconsistent shard count in %s" % path)
                bloom.shards[int(shard)] = BloomFilter.decode(encoded)
        if bloom is None:
            raise ValueError("Bloom filter file %s is empty" % path)
        return bloom
