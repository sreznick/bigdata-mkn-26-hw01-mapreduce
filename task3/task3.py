# task3.py
from collections import defaultdict
from datetime import date
from decimal import Decimal

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

HEADER = "transaction_id,user_id,amount,timestamp"
THRESHOLD = Decimal(10000)


class SuspiciousTransactions(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        line = line.strip()
        if not line or line == HEADER:
            return
        transaction_id, user_id, amount, timestamp = line.split(",")
        day = date.fromisoformat(timestamp[:10]).toordinal()
        yield user_id, [day, transaction_id, amount]

    def reducer(self, user_id, transactions):
        # у клиента не больше 1000 переводов, группируем по дням в памяти
        by_day = defaultdict(list)
        for day, transaction_id, amount in transactions:
            by_day[day].append((Decimal(amount), transaction_id))

        for day in by_day.values():
            day.sort(reverse=True)

        for day, earlier in by_day.items():
            later = by_day.get(day + 1)
            if not later:
                continue
            for amount_1, transaction_id_1 in earlier:
                for amount_2, transaction_id_2 in later:
                    total = amount_1 + amount_2
                    if total <= THRESHOLD:
                        # later отсортирован по убыванию, дальше суммы только меньше
                        break
                    yield None, "%s,%s,%s,%s" % (user_id, transaction_id_1,
                                                 transaction_id_2,
                                                 format(total, "f"))


if __name__ == '__main__':
    SuspiciousTransactions.run()
