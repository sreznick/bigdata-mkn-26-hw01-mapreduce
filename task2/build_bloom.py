# build_bloom.py
# Строит фильтр Блума по справочнику устройств (файл A) для task2.py --bloom.
# Запускается раз в сутки, после обновления справочника.
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

from bloom import BloomFilter, key_hashes

A_HEADER = "device_id,trust_category"


class BuildDeviceBloomFilter(MRJob):

    FILES = ["bloom.py"]
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--capacity", type=int, default=10 ** 9,
                              help="Expected number of devices")
        self.add_passthru_arg("--error-rate", type=float, default=0.01,
                              help="Target false positive rate")
        self.add_passthru_arg("--shards", type=int, default=64,
                              help="Number of filter shards (one reducer each)")

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        line = line.strip()
        if not line or line == A_HEADER:
            return
        device_id = line.split(",")[0]
        h1, h2, h3 = key_hashes(device_id)
        yield h3 % self.options.shards, [h1, h2]

    def reducer(self, shard, hashes):
        bloom = BloomFilter.for_capacity(
            self.options.capacity // self.options.shards + 1,
            self.options.error_rate)
        for h1, h2 in hashes:
            bloom.add_hashes(h1, h2)
        yield None, "%d,%d,%s" % (shard, self.options.shards, bloom.encode())


if __name__ == '__main__':
    BuildDeviceBloomFilter.run()
