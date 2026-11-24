# task1.py
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

TOP_N = 3
KEEP = TOP_N + 1


class TopSummary(object):
    __slots__ = ('entries', 'total', 'full', 'floor')

    def __init__(self):
        self.entries = {}
        self.total = 0
        self.full = False
        self.floor = None

    def add(self, value, count, row_ids):
        if self.full and value < self.floor:
            return
        entry = self.entries.get(value)
        if entry is None:
            entry = self.entries[value] = [0, []]
        entry[0] += count
        free = TOP_N - len(entry[1])
        if free > 0:
            entry[1].extend(row_ids[:free])
        self.total += count
        if not (self.full and value == self.floor):
            self._shrink()

    def _shrink(self):
        if self.total < KEEP:
            return
        covered = 0
        for value in sorted(self.entries, reverse=True):
            if covered >= KEEP:
                self.total -= self.entries.pop(value)[0]
            else:
                covered += self.entries[value][0]
                self.floor = value
        self.full = True

    def encode(self):
        return [[repr(value), count, ids]
                for value, (count, ids) in self.entries.items()]

    def merge(self, encoded):
        for value, count, row_ids in encoded:
            self.add(float(value), count, row_ids)

    def result(self):
        ordered = sorted(self.entries.items(), key=lambda item: item[0],
                         reverse=True)
        places = []
        for value, (count, _) in ordered:
            places.extend([value] * min(count, KEEP - len(places)))
            if len(places) == KEEP:
                break

        if len(places) < KEEP:
            top_ids = [row_id for _, (_, ids) in ordered for row_id in sorted(ids)]
            return top_ids, 0

        third, fourth = places[TOP_N - 1], places[TOP_N]
        top_ids = [row_id for value, (_, ids) in ordered if value > fourth
                   for row_id in sorted(ids)]

        border = self.entries[third][0] if third == fourth else 0
        return top_ids, border


class Top3WithBorder(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer=self.reducer)
        ]

    def mapper_init(self):
        self.summaries = {}

    def mapper(self, _, line):
        fields = line.strip().split(',')
        if len(fields) < 2:
            return
        try:
            values = [float(x) for x in fields[1:]]
        except ValueError:
            return
        row_id = (fields[0],)
        for metric, value in enumerate(values, 1):
            if value != value:
                continue
            summary = self.summaries.get(metric)
            if summary is None:
                summary = self.summaries[metric] = TopSummary()
            summary.add(value, 1, row_id)

    def mapper_final(self):
        for metric, summary in self.summaries.items():
            yield metric, summary.encode()

    def reducer(self, metric, encoded_summaries):
        summary = TopSummary()
        for encoded in encoded_summaries:
            summary.merge(encoded)
        top_ids, border = summary.result()
        yield None, f'{metric},{";".join(top_ids)},{border}'


if __name__ == '__main__':
    Top3WithBorder.run()
