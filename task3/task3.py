# task3.py
from bisect import bisect_right
from datetime import date
from decimal import Decimal

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

THRESHOLD = Decimal('10000')


class SuspiciousTransactions(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        parts = line.strip().split(',')
        if len(parts) != 4 or parts[0] == 'transaction_id':
            return
        transaction_id, user_id, amount, timestamp = parts
        day = date.fromisoformat(timestamp[:10]).toordinal()
        yield user_id, (day, amount, transaction_id)

    def reducer(self, user_id, values):
        days = {}
        for day, amount, transaction_id in values:
            days.setdefault(day, []).append((Decimal(amount), transaction_id))
        for transfers in days.values():
            transfers.sort()

        for day, today in days.items():
            tomorrow = days.get(day + 1)
            if not tomorrow:
                continue
            amounts = [amount for amount, _ in tomorrow]
            for amount_1, id_1 in today:
                start = bisect_right(amounts, THRESHOLD - amount_1)
                for amount_2, id_2 in tomorrow[start:]:
                    yield None, f'{user_id},{id_1},{id_2},{amount_1 + amount_2:f}'


if __name__ == '__main__':
    SuspiciousTransactions.run()