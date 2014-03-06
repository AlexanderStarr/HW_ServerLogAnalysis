# Written by Alexander Starr, student ID 00567613

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class Apache(MRJob):

    def mapper(self, _, line):
        elems = line.split()
        client = elems[0]
        time_stamp = elems[3][1:]
        offset = elems[4][:-1]
        unix_time = time.mktime(time.strptime(time_stamp, '%d/%b/%Y:%H:%M:%S'))
        yield (client, unix_time)

    def reducer(self, client, timevector):
        s_times = sorted(list(timevector))
        time_delta = s_times[len(s_times)-1] - s_times[0]
        yield (client, time_delta)

if __name__ == '__main__':
    Apache.run()
