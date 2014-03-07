# Written by Alexander Starr, student ID 00567613

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class Apache(MRJob):

    def mapper1(self, _, line):
        elems = line.strip().split()
        client = elems[0]
        time_stamp = elems[3][1:]
        offset = elems[4][:-1]
        sign = float(offset[0:1] + '1')
        offset_sec = sign * float(offset[1:3]) * 3600 + sign * float(offset[3:]) * 60
        unix_time = time.mktime(time.strptime(time_stamp, '%d/%b/%Y:%H:%M:%S')) + offset_sec
        yield (client, unix_time)

    def reducer1(self, client, timevector):
        s_times = sorted(list(timevector))
        time_delta = s_times[len(s_times)-1] - s_times[0]
        yield (client, time_delta)

    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1)]

if __name__ == '__main__':
    Apache.run()
