# Written by Alexander Starr, student ID 00567613

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime as dt

class Apache(MRJob):

    def mapper(self, _, line):
        client = line.split()[0]
        time_stamp = line.split('[')[1].split(']')[0]
        unix_time = time_stamp
        yield (client, time_stamp)

    def reducer(self, client, timevector):
        yield (client, len(list(timevector)))

if __name__ == '__main__':
    Apache.run()
