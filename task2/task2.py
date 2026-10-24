# task2.py
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

from bloom import ShardedBloomFilter

A_HEADER = "device_id,trust_category"
B_HEADER = "event_id,device_id,event_type"


class EnrichEventsWithDeviceInfo(MRJob):

    FILES = ["bloom.py"]
    OUTPUT_PROTOCOL = RawValueProtocol
    # запись справочника ("A", ...) приходит в reducer раньше событий ("B", ...)
    SORT_VALUES = True

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--bloom",
                          help="Bloom filter of known devices from build_bloom.py")

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper_init(self):
        self.bloom = None
        if self.options.bloom:
            self.bloom = ShardedBloomFilter.load(self.options.bloom)

    def mapper(self, _, line):
        line = line.strip()
        if not line or line in (A_HEADER, B_HEADER):
            return
        fields = line.split(",")
        if len(fields) == 2:
            device_id, trust_category = fields
            yield device_id, ["A", trust_category]
            return
        event_id, device_id, event_type = fields
        if not device_id:
            return
        if self.bloom is not None and device_id not in self.bloom:
            return
        yield device_id, ["B", event_id, event_type]

    def reducer(self, device_id, values):
        first = next(values)
        if first[0] != "A":
            return
        trust_category = first[1]
        for _tag, event_id, event_type in values:
            yield None, "%s,%s,%s,%s" % (event_id, device_id, event_type,
                                         trust_category)


if __name__ == '__main__':
    EnrichEventsWithDeviceInfo.run()
