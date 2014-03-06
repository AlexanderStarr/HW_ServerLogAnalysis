# Written by Alexander Starr, student ID 00567613

from mrjob.job import MRJob
from mrjob.step import MRStep

class Apache(MRJob):

    def mapper(self, _, line):
        line_elems = line.split()
        client = line_elems[0]
        time = line_elems[3]
        yield (client, time)

    def reducer(self, client, timevector):
        yield (client, len(list(timevector)))

if __name__ == '__main__':
    Apache.run()
