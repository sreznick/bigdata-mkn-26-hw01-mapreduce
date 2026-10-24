# task1.py
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

METRICS_COUNT = 9
TOP = 3

def add_to_summary(summary, value, count, ids):
    above = 0
    for i, entry in enumerate(summary):
        if above > TOP:
            del summary[i:]
            return
        if value == entry[0]:
            entry[1] += count
            entry[2].extend(ids[:TOP - len(entry[2])])
            break
        if value > entry[0]:
            summary.insert(i, [value, count, ids[:TOP]])
            break
        above += entry[1]
    else:
        if above <= TOP:
            summary.append([value, count, ids[:TOP]])
        return

    above = 0
    for i, entry in enumerate(summary):
        if above > TOP:
            del summary[i:]
            return
        above += entry[1]


def merge_summaries(summaries):
    result = []
    for summary in summaries:
        for value, count, ids in summary:
            add_to_summary(result, float(value), count, ids)
    return result


def encode(summary):
    # repr() сохраняет float без потерь и не зависит от поддержки inf в JSON
    return [[repr(value), count, ids] for value, count, ids in summary]


class Top3WithBorder(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper_init(self):
        self.summaries = [[] for _ in range(METRICS_COUNT)]

    def mapper(self, _, line):
        row_id, *metrics = line.strip().split(",")
        try:
            values = [float(m) for m in metrics]
        except ValueError:
            # строка заголовка
            return
        for summary, value in zip(self.summaries, values):
            if value != value:
                # NaN не сравним ни с чем, в рейтинге не участвует
                continue
            add_to_summary(summary, value, 1, [row_id])

    def mapper_final(self):
        for i, summary in enumerate(self.summaries):
            yield i + 1, encode(summary)

    def combiner(self, metric_id, summaries):
        yield metric_id, encode(merge_summaries(summaries))

    def reducer(self, metric_id, summaries):
        summary = merge_summaries(summaries)

        third = fourth = None
        position = 0
        for value, count, _ids in summary:
            if position < TOP <= position + count:
                third = value
            if position < TOP + 1 <= position + count:
                fourth = value
            position += count

        top_ids = []
        for value, _count, ids in summary:
            if fourth is not None and value <= fourth:
                break
            top_ids.extend(sorted(ids))

        border_count = 0
        if fourth is not None and fourth == third:
            border_count = next(c for v, c, _ in summary if v == third)

        yield None, "%d,%s,%d" % (metric_id, ";".join(top_ids), border_count)


if __name__ == '__main__':
    Top3WithBorder.run()
